use calamine::{Reader, Xlsx, Data};
use eframe::egui;
use mongodb::{Client, options::ClientOptions, bson::{doc, Document, Bson}};
use rust_xlsxwriter::{Workbook};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

// --- Estructuras de Configuración ---

#[derive(Clone, Debug, PartialEq)]
enum FilterOperator {
    Equals,      // Exacto (pero case insensitive)
    Contains,    // Contiene
    NotEquals,   // No es igual
    StartsWith,  // Empieza por (útil para teléfonos 0414...)
}

#[derive(Clone, Debug)]
struct StaticFilter {
    field: String,
    operator: FilterOperator,
    value: String,
}

#[derive(Clone, Debug)]
struct MatchRule {
    col_letter: String, 
    row_num: u32,       
    mongo_field: String,
    use_last_8: bool,
}

#[derive(Clone, Debug)]
struct FillRule {
    mongo_field: String,     
    col_letter: String, 
    row_num: u32,       
    apply_format: bool,
    is_lookup: bool,         
    lookup_coll: String,     
    lookup_target: String,   
}

struct MyApp {
    mongo_uri: String,
    db_name: String,
    collection_name: String,
    
    input_path: String,
    output_path: String,

    match_rules: Vec<MatchRule>,
    static_filters: Vec<StaticFilter>, // <--- NUEVO: Filtros fijos
    fill_rules: Vec<FillRule>,

    status_log: Arc<Mutex<String>>,
    is_processing: bool,
    rt: Runtime,
}

impl Default for MyApp {
    fn default() -> Self {
        Self {
            mongo_uri: "mongodb://localhost:27017".to_owned(),
            db_name: "".to_owned(),
            collection_name: "".to_owned(),
            input_path: "".to_owned(),
            output_path: "resultado.xlsx".to_owned(),
            
            match_rules: vec![],
            static_filters: vec![], // Inicializar vacío
            fill_rules: vec![],
            
            status_log: Arc::new(Mutex::new("Esperando configuración...".to_string())),
            is_processing: false,
            rt: Runtime::new().unwrap(),
        }
    }
}

// --- Interfaz Gráfica ---

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.heading("Cruce Experto MongoDB <-> Excel (Pro)");
                ui.add_space(10.0);
                ui.separator();

                // 1. Configuración Mongo
                ui.collapsing("1. Conexión MongoDB", |ui| {
                    ui.add_space(5.0);
                    ui.label("URI de Conexión:");
                    ui.text_edit_singleline(&mut self.mongo_uri);
                    ui.add_space(5.0);
                    ui.horizontal(|ui| {
                        ui.label("Base de Datos:"); 
                        ui.text_edit_singleline(&mut self.db_name);
                        ui.add_space(10.0);
                        ui.label("Colección:"); 
                        ui.text_edit_singleline(&mut self.collection_name);
                    });
                    ui.add_space(5.0);
                });

                ui.add_space(10.0);

                // 2. Archivos
                ui.collapsing("2. Archivos", |ui| {
                    ui.add_space(5.0);
                    ui.horizontal(|ui| {
                        ui.label("Excel Origen:");
                        ui.text_edit_singleline(&mut self.input_path);
                        if ui.button("📂").clicked() {
                            if let Some(path) = rfd::FileDialog::new().pick_file() {
                                self.input_path = path.display().to_string();
                            }
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label("Excel Destino:");
                        ui.text_edit_singleline(&mut self.output_path);
                    });
                });

                ui.separator();
                ui.add_space(10.0);

                // 3. Match
                ui.label(egui::RichText::new("3. Identificación (Match)").strong().size(16.0));
                ui.label("Columnas del Excel para encontrar el documento único.");
                
                let mut remove_match_idx = None;
                for (i, rule) in self.match_rules.iter_mut().enumerate() {
                    ui.horizontal(|ui| {
                        ui.label("Col:"); ui.add(egui::TextEdit::singleline(&mut rule.col_letter).desired_width(30.0));
                        ui.label("Fila:"); ui.add(egui::DragValue::new(&mut rule.row_num));
                        ui.label("== Mongo Campo:"); ui.text_edit_singleline(&mut rule.mongo_field);
                        // AQUI ACLARAMOS EL TEXTO PARA QUE SEPAS QUE HACE
                        ui.checkbox(&mut rule.use_last_8, "✂ 8 Dig + Mayús");
                        if ui.button("❌").clicked() { remove_match_idx = Some(i); }
                    });
                }
                if let Some(i) = remove_match_idx { self.match_rules.remove(i); }
                if ui.button("➕ Agregar Criterio Match").clicked() {
                    self.match_rules.push(MatchRule { col_letter: "A".into(), row_num: 2, mongo_field: "_id".into(), use_last_8: false });
                }

                ui.add_space(15.0);
                ui.separator();

                // 4. Filtros Estáticos
                ui.label(egui::RichText::new("4. Filtros Globales (Condiciones Fijas)").strong().size(16.0));
                ui.label("Estos filtros se aplican SIEMPRE (Insensible a mayúsculas).");
                ui.add_space(5.0);

                let mut remove_filter_idx = None;
                for (i, filter) in self.static_filters.iter_mut().enumerate() {
                    ui.group(|ui| {
                        ui.horizontal(|ui| {
                            ui.label("Campo Mongo:");
                            ui.text_edit_singleline(&mut filter.field);
                            
                            // CORRECCION DEL WARNING AQUI: from_id_salt
                            egui::ComboBox::from_id_salt(i) 
                                .selected_text(match filter.operator {
                                    FilterOperator::Equals => "Igual a (=)",
                                    FilterOperator::Contains => "Contiene (LIKE)",
                                    FilterOperator::NotEquals => "Diferente de (!=)",
                                    FilterOperator::StartsWith => "Empieza por (^...)",
                                })
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut filter.operator, FilterOperator::Equals, "Igual a");
                                    ui.selectable_value(&mut filter.operator, FilterOperator::Contains, "Contiene");
                                    ui.selectable_value(&mut filter.operator, FilterOperator::NotEquals, "Diferente de");
                                    ui.selectable_value(&mut filter.operator, FilterOperator::StartsWith, "Empieza por");
                                });

                            ui.label("Valor:");
                            ui.text_edit_singleline(&mut filter.value);

                            if ui.button("🗑").clicked() { remove_filter_idx = Some(i); }
                        });
                    });
                }
                if let Some(i) = remove_filter_idx { self.static_filters.remove(i); }
                
                if ui.button("➕ Agregar Filtro (Ej: sState = Activo)").clicked() {
                    self.static_filters.push(StaticFilter { 
                        field: "sState".into(), 
                        operator: FilterOperator::Equals, 
                        value: "Activo".into() 
                    });
                }

                ui.add_space(15.0);
                ui.separator();

                // 5. Salida
                ui.label(egui::RichText::new("5. Salida (Relleno)").strong().size(16.0));
                
                let mut remove_fill_idx = None;
                for (i, rule) in self.fill_rules.iter_mut().enumerate() {
                    ui.group(|ui| {
                        ui.horizontal(|ui| {
                            ui.label("Mongo:"); ui.text_edit_singleline(&mut rule.mongo_field);
                            ui.label("-> Col:"); ui.add(egui::TextEdit::singleline(&mut rule.col_letter).desired_width(30.0));
                            ui.label("Fila:"); ui.add(egui::DragValue::new(&mut rule.row_num));
                            if ui.button("🗑").clicked() { remove_fill_idx = Some(i); }
                        });
                        ui.horizontal(|ui| {
                            // AQUI TAMBIEN ACLARAMOS EL TEXTO
                            ui.checkbox(&mut rule.apply_format, "Convertir (8 Dig + Mayús)");
                            ui.checkbox(&mut rule.is_lookup, "Relación (Lookup)");
                        });
                        if rule.is_lookup {
                            ui.indent("lookup", |ui| {
                                ui.horizontal(|ui| {
                                    ui.label("Coll:"); ui.text_edit_singleline(&mut rule.lookup_coll);
                                    ui.label("Campo:"); ui.text_edit_singleline(&mut rule.lookup_target);
                                });
                            });
                        }
                    });
                }
                if let Some(i) = remove_fill_idx { self.fill_rules.remove(i); }
                if ui.button("➕ Agregar Campo Salida").clicked() {
                    self.fill_rules.push(FillRule { 
                        mongo_field: "".into(), col_letter: "B".into(), row_num: 1, 
                        apply_format: false, is_lookup: false, lookup_coll: "".into(), lookup_target: "".into() 
                    });
                }

                ui.add_space(20.0);
                ui.separator();

                if self.is_processing {
                    ui.spinner();
                } else {
                    if ui.button(egui::RichText::new("🚀 EJECUTAR").size(20.0)).clicked() {
                        self.start_processing();
                    }
                }

                ui.add_space(10.0);
                egui::Frame::dark_canvas(ui.style()).show(ui, |ui| {
                    ui.set_min_height(100.0);
                    ui.set_width(ui.available_width());
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        let log = self.status_log.lock().unwrap();
                        ui.monospace(log.as_str());
                    });
                });
            });
        });
    }
}
impl MyApp {
    fn start_processing(&mut self) {
        if self.db_name.is_empty() || self.collection_name.is_empty() {
             *self.status_log.lock().unwrap() = "❌ ERROR: Faltan datos de BD.".to_string();
             return;
        }

        self.is_processing = true;
        let log = self.status_log.clone();
        
        // Clonar datos
        let uri = self.mongo_uri.clone();
        let db = self.db_name.clone();
        let coll = self.collection_name.clone();
        let input = self.input_path.clone();
        let output = self.output_path.clone();
        let m_rules = self.match_rules.clone();
        let s_filters = self.static_filters.clone(); // Clonamos filtros
        let f_rules = self.fill_rules.clone();

        self.rt.spawn(async move {
            match run_logic(uri, db, coll, input, output, m_rules, s_filters, f_rules, log.clone()).await {
                Ok(path) => { *log.lock().unwrap() = format!("✅ COMPLETADO.\nArchivo: {}", path); },
                Err(e) => { *log.lock().unwrap() = format!("❌ ERROR: {}", e); }
            }
        });
        self.is_processing = false; 
    }
}

// --- Motor Lógico ---

async fn run_logic(
    uri: String, db_name: String, coll_name: String, 
    input_path: String, output_path: String,
    match_rules: Vec<MatchRule>, 
    static_filters: Vec<StaticFilter>, // RECIBIR FILTROS
    fill_rules: Vec<FillRule>,
    log: Arc<Mutex<String>>
) -> anyhow::Result<String> { 
    
    // Conexión
    let client = Client::with_options(ClientOptions::parse(&uri).await?)?;
    let db = client.database(&db_name);
    let main_collection = db.collection::<Document>(&coll_name);

    // Leer Excel
    let mut workbook: Xlsx<_> = calamine::open_workbook(PathBuf::from(&input_path))
        .map_err(|e| anyhow::anyhow!("Error Excel: {}", e))?;
    let range = workbook.worksheet_range_at(0).ok_or(anyhow::anyhow!("Excel vacío"))??;

    // Preparar Reglas Match
    let mut parsed_match_rules = Vec::new();
    let mut max_header_row_idx = 0;
    for rule in &match_rules {
        let col_idx = col_letter_to_index(&rule.col_letter)?;
        let row_idx = if rule.row_num > 0 { rule.row_num as usize - 1 } else { 0 };
        if row_idx > max_header_row_idx { max_header_row_idx = row_idx; }
        parsed_match_rules.push((col_idx, rule));
    }

    // Preparar Reglas Salida
    let mut parsed_fill_rules = Vec::new();
    for rule in &fill_rules {
        let col_idx = col_letter_to_index(&rule.col_letter)?;
        parsed_fill_rules.push((col_idx, rule));
    }

    // --- CONSTRUIR FILTRO BASE (ESTÁTICO) ---
    // Este filtro se aplicará a TODAS las búsquedas
    let mut base_filter_doc = doc! {};
    
    for filter in static_filters {
        if !filter.field.is_empty() {
            let safe_value = regex::escape(&filter.value);
            
            let condition = match filter.operator {
                FilterOperator::Equals => {
                    // ^valor$ con i (insensible)
                    doc! { "$regex": format!("^{}$", safe_value), "$options": "i" }
                },
                FilterOperator::Contains => {
                    // valor con i
                    doc! { "$regex": format!("{}", safe_value), "$options": "i" }
                },
                FilterOperator::StartsWith => {
                    // ^valor con i
                    doc! { "$regex": format!("^{}", safe_value), "$options": "i" }
                },
                FilterOperator::NotEquals => {
                    // $not { $regex: ^valor$, $options: i }
                    doc! { "$not": { "$regex": format!("^{}$", safe_value), "$options": "i" } }
                }
            };
            
            base_filter_doc.insert(filter.field, condition);
        }
    }

    // Copiar Excel
    let mut new_workbook = Workbook::new();
    let worksheet = new_workbook.add_worksheet();
    for (row_idx, row) in range.rows().enumerate() {
        for (col_idx, cell) in row.iter().enumerate() {
            write_cell(worksheet, row_idx as u32, col_idx as u16, cell)?;
        }
    }

    let start_data_row = max_header_row_idx + 1;
    let total_rows = range.height();
    let mut matches_found = 0;

    { let mut l = log.lock().unwrap(); *l = format!("Iniciando con {} filtros globales...", base_filter_doc.len()); }

    for current_row_idx in start_data_row..total_rows {
        
        // 1. Empezamos con el filtro base (Estado activo, telefono, etc)
        let mut filter = base_filter_doc.clone();
        let mut has_match_criteria = false;

        // 2. Agregamos criterios dinámicos (fila actual)
        for (col_idx, rule) in &parsed_match_rules {
            if let Some(val) = range.get_value((current_row_idx as u32, *col_idx as u32)) {
                let mut str_val = get_string_val(val);
                if !str_val.trim().is_empty() {
                    if rule.use_last_8 { str_val = format_id_string(&str_val); } 
                    else { str_val = str_val.trim().to_string(); }

                    // Búsqueda dinámica también case insensitive
                    let regex_pattern = format!("{}", regex::escape(&str_val)); 
                    filter.insert(&rule.mongo_field, doc! { "$regex": regex_pattern, "$options": "i" });
                    has_match_criteria = true;
                }
            }
        }

        // Solo buscar si hay criterio de fila Y el filtro base no está vacío (o si se permite match solo por base)
        if has_match_criteria {
            if let Ok(Some(doc)) = main_collection.find_one(filter).await {
                matches_found += 1;

                // Rellenar datos
                for (target_col, rule) in &parsed_fill_rules {
                    if let Some(val_origin) = doc.get(&rule.mongo_field) {
                        let mut final_value = if rule.is_lookup {
                            // Lookup simple
                            let lookup_coll = db.collection::<Document>(&rule.lookup_coll);
                            if let Ok(Some(related)) = lookup_coll.find_one(doc!{"_id": val_origin.clone()}).await {
                                if let Some(res) = related.get(&rule.lookup_target) {
                                    bson_to_string_smart(res)
                                } else { "".to_string() }
                            } else { "".to_string() }
                        } else {
                            bson_to_string_smart(val_origin)
                        };

                        if rule.apply_format { final_value = format_id_string(&final_value); }
                        worksheet.write_string(current_row_idx as u32, *target_col as u16, final_value)?;
                    }
                }
            }
        }

        if current_row_idx % 50 == 0 {
             let mut l = log.lock().unwrap(); 
             *l = format!("Progreso: {}/{} | Match: {}", current_row_idx, total_rows, matches_found);
        }
    }

    new_workbook.save(&output_path)?;
    Ok(std::fs::canonicalize(&output_path).unwrap_or(PathBuf::from(&output_path)).display().to_string())
}

// --- Helpers sin cambios mayores ---
fn format_id_string(input: &str) -> String {
    let mut s = input.trim().to_uppercase();
    if s.len() > 8 { s = s[s.len()-8..].to_string(); }
    s
}
fn col_letter_to_index(letter: &str) -> anyhow::Result<usize> {
    let letter = letter.trim().to_uppercase();
    let mut col_idx: usize = 0;
    for c in letter.chars() {
        if !c.is_alphabetic() { return Err(anyhow::anyhow!("Columna mal: {}", letter)); }
        col_idx = col_idx * 26 + (c as usize - 'A' as usize + 1);
    }
    Ok(col_idx - 1)
}
fn write_cell(sheet: &mut rust_xlsxwriter::Worksheet, row: u32, col: u16, data: &Data) -> anyhow::Result<()> {
    match data {
        Data::Int(v) => { sheet.write_number(row, col, *v as f64)?; },
        Data::Float(v) => { sheet.write_number(row, col, *v)?; },
        Data::String(v) => { sheet.write_string(row, col, v)?; },
        Data::Bool(v) => { sheet.write_boolean(row, col, *v)?; },
        _ => { sheet.write_string(row, col, data.to_string())?; }, 
    };
    Ok(())
}
fn get_string_val(data: &Data) -> String {
    match data {
        Data::Int(v) => v.to_string(),
        Data::Float(v) => if v.fract() == 0.0 { (*v as i64).to_string() } else { v.to_string() },
        Data::String(v) => v.clone(),
        _ => data.to_string(),
    }
}
fn bson_to_string_smart(val: &Bson) -> String {
    match val {
        Bson::String(s) => s.clone(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => f.to_string(),
        Bson::ObjectId(oid) => oid.to_string(),
        _ => format!("{}", val).replace("\"", ""), 
    }
}
fn main() -> eframe::Result<()> {
    eframe::run_native(
        "Excel-Mongo Pro",
        eframe::NativeOptions { viewport: eframe::egui::ViewportBuilder::default().with_inner_size([900.0, 850.0]), ..Default::default() },
        Box::new(|_cc| Ok(Box::new(MyApp::default()))),
    )
}