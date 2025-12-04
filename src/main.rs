use calamine::{Reader, Xlsx, Data};
use eframe::egui;
use mongodb::{Client, options::ClientOptions, bson::{doc, Document, Bson}};
use rust_xlsxwriter::{Workbook};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

// --- Estructuras de Configuración ---

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
    
    // Opciones de Transformación al Escribir
    apply_format: bool, // <--- NUEVO: Aplicar mayúsculas y 8 dígitos al guardar

    // Lógica Relacional
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
    fill_rules: Vec<FillRule>,

    status_log: Arc<Mutex<String>>,
    is_processing: bool,
    rt: Runtime,
}

impl Default for MyApp {
    fn default() -> Self {
        Self {
            // VALORES POR DEFECTO VACÍOS (A petición)
            mongo_uri: "mongodb://localhost:27017".to_owned(), // Dejo localhost por comodidad, bórralo si prefieres vacío total
            db_name: "".to_owned(),
            collection_name: "".to_owned(),
            input_path: "".to_owned(),
            output_path: "resultado.xlsx".to_owned(),
            
            match_rules: vec![], // Empieza sin reglas
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
                ui.heading("Cruce Experto MongoDB <-> Excel");
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
                        ui.text_edit_singleline(&mut self.db_name).on_hover_text("Nombre de la DB");
                        ui.add_space(10.0);
                        ui.label("Colección:"); 
                        ui.text_edit_singleline(&mut self.collection_name).on_hover_text("Colección principal");
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
                    ui.add_space(5.0);
                    ui.horizontal(|ui| {
                        ui.label("Excel Destino:");
                        ui.text_edit_singleline(&mut self.output_path);
                    });
                    ui.add_space(5.0);
                });

                ui.separator();
                ui.add_space(10.0);

                // 3. Match
                ui.label(egui::RichText::new("3. Criterios de Búsqueda (Match)").strong().size(16.0));
                ui.label("¿Qué columnas del Excel usamos para buscar en Mongo?");
                ui.add_space(5.0);
                
                let mut remove_idx = None;
                for (i, rule) in self.match_rules.iter_mut().enumerate() {
                    ui.group(|ui| {
                        ui.horizontal(|ui| {
                            ui.label("Col (Letra):");
                            ui.add(egui::TextEdit::singleline(&mut rule.col_letter).desired_width(40.0));
                            
                            ui.label("Fila Inicio:");
                            ui.add(egui::DragValue::new(&mut rule.row_num).speed(0.1));

                            ui.label("== Mongo Campo:");
                            ui.text_edit_singleline(&mut rule.mongo_field);
                            
                            if ui.button("❌").clicked() { remove_idx = Some(i); }
                        });
                        ui.checkbox(&mut rule.use_last_8, "✂ Formatear al Buscar (Solo 8 últimos + Mayúsculas)");
                    });
                    ui.add_space(5.0);
                }
                if let Some(i) = remove_idx { self.match_rules.remove(i); }
                if ui.button("➕ Agregar Criterio de Búsqueda").clicked() {
                    self.match_rules.push(MatchRule { 
                        col_letter: "".to_string(), 
                        row_num: 1,
                        mongo_field: "".to_string(),
                        use_last_8: false
                    });
                }

                ui.add_space(15.0);
                ui.separator();
                ui.add_space(15.0);

                // 4. Datos a Rellenar
                ui.label(egui::RichText::new("4. Datos a Rellenar (Salida)").strong().size(16.0));
                ui.label("¿Qué datos sacamos de Mongo y dónde los ponemos?");
                ui.add_space(5.0);

                let mut remove_fill_idx = None;
                for (i, rule) in self.fill_rules.iter_mut().enumerate() {
                    ui.group(|ui| {
                        ui.horizontal(|ui| {
                            ui.label("Mongo Campo:");
                            ui.text_edit_singleline(&mut rule.mongo_field);
                            
                            ui.label("-> Destino Col:");
                            ui.add(egui::TextEdit::singleline(&mut rule.col_letter).desired_width(40.0));
                            
                            ui.label("Fila Header:");
                            ui.add(egui::DragValue::new(&mut rule.row_num).speed(0.1));

                            if ui.button("🗑").clicked() { remove_fill_idx = Some(i); }
                        });
                        
                        ui.horizontal(|ui| {
                            ui.checkbox(&mut rule.apply_format, "🔠 Formatear al Escribir (8 dig + Mayús)");
                            ui.checkbox(&mut rule.is_lookup, "🔗 Es Relación (Lookup)");
                        });

                        if rule.is_lookup {
                            ui.indent("lookup", |ui| {
                                ui.horizontal(|ui| {
                                    ui.label("Colección:"); ui.text_edit_singleline(&mut rule.lookup_coll);
                                    ui.label("Campo a traer:"); ui.text_edit_singleline(&mut rule.lookup_target);
                                });
                            });
                        }
                    });
                    ui.add_space(5.0);
                }
                if let Some(i) = remove_fill_idx { self.fill_rules.remove(i); }
                if ui.button("➕ Agregar Campo de Relleno").clicked() {
                    self.fill_rules.push(FillRule { 
                        mongo_field: "".to_string(), 
                        col_letter: "".to_string(),
                        row_num: 1, 
                        apply_format: false,
                        is_lookup: false, lookup_coll: "".to_string(), lookup_target: "".to_string() 
                    });
                }

                ui.add_space(20.0);
                ui.separator();
                ui.add_space(10.0);

                if self.is_processing {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Procesando... Verifica el Log abajo para ver progreso.");
                    });
                } else {
                    if ui.button(egui::RichText::new("🚀 EJECUTAR PROCESO").size(20.0)).clicked() {
                        self.start_processing();
                    }
                }

                ui.add_space(10.0);
                ui.label("Log de Estado:");
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
        // Validaciones básicas antes de lanzar el hilo
        if self.db_name.is_empty() || self.collection_name.is_empty() {
             *self.status_log.lock().unwrap() = "❌ ERROR: Debes indicar Base de Datos y Colección.".to_string();
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
        let f_rules = self.fill_rules.clone();
        let _ctx = egui::Context::default(); 

        self.rt.spawn(async move {
            match run_logic(uri, db, coll, input, output, m_rules, f_rules, log.clone()).await {
                Ok(path) => { 
                    let mut l = log.lock().unwrap(); 
                    *l = format!("{}\n✅ PROCESO COMPLETADO.\n📂 Archivo guardado en:\n{}", *l, path); 
                },
                Err(e) => { 
                    let mut l = log.lock().unwrap(); 
                    *l = format!("{}\n❌ ERROR CRÍTICO: {}", *l, e); 
                }
            }
        });
        self.is_processing = false; 
    }
}

// --- Motor Lógico ---

async fn run_logic(
    uri: String, db_name: String, coll_name: String, 
    input_path: String, output_path: String,
    match_rules: Vec<MatchRule>, fill_rules: Vec<FillRule>,
    log: Arc<Mutex<String>>
) -> anyhow::Result<String> { // Retorna String (ruta) en éxito
    
    // 1. Validar Conexión (Ping)
    { let mut l = log.lock().unwrap(); *l = "Intentando conectar a MongoDB...".to_string(); }
    
    let client_options = match ClientOptions::parse(&uri).await {
        Ok(opts) => opts,
        Err(e) => return Err(anyhow::anyhow!("Error en la URI: {}", e)),
    };
    
    let client = Client::with_options(client_options)?;
    
    // Test real de conexión
    if let Err(e) = client.list_database_names().await {
        return Err(anyhow::anyhow!("No se pudo conectar al servidor Mongo. Revisa la URI o tu VPN/Red.\nDetalle: {}", e));
    }

    let db = client.database(&db_name);
    let main_collection = db.collection::<Document>(&coll_name);

    // 2. Leer Excel
    { let mut l = log.lock().unwrap(); *l = format!("{}\nConectado. Leyendo Excel...", *l); }
    let mut workbook: Xlsx<_> = calamine::open_workbook(PathBuf::from(&input_path))
        .map_err(|e| anyhow::anyhow!("No se pudo abrir el Excel. ¿La ruta es correcta? ¿Está abierto?\nError: {}", e))?;
    
    let range = workbook.worksheet_range_at(0).ok_or(anyhow::anyhow!("El Excel no tiene hojas."))??;

    // 3. Preparar Reglas
    let mut max_header_row_idx = 0;
    
    let mut parsed_match_rules = Vec::new();
    for rule in &match_rules {
        let col_idx = col_letter_to_index(&rule.col_letter)?;
        let row_idx = if rule.row_num > 0 { rule.row_num as usize - 1 } else { 0 };
        if row_idx > max_header_row_idx { max_header_row_idx = row_idx; }
        parsed_match_rules.push((col_idx, row_idx, rule));
    }

    let mut parsed_fill_rules = Vec::new();
    for rule in &fill_rules {
        let col_idx = col_letter_to_index(&rule.col_letter)?;
        parsed_fill_rules.push((col_idx, rule));
    }

    // 4. Preparar Salida
    let mut new_workbook = Workbook::new();
    let worksheet = new_workbook.add_worksheet();

    // Copiar Original
    for (row_idx, row) in range.rows().enumerate() {
        for (col_idx, cell) in row.iter().enumerate() {
            write_cell(worksheet, row_idx as u32, col_idx as u16, cell)?;
        }
    }

    // 5. Iterar Datos
    let start_data_row = max_header_row_idx + 1;
    let total_rows = range.height();
    let mut matches_found = 0;

    { let mut l = log.lock().unwrap(); *l = format!("Analizando desde fila {} hasta {}.\n(Si el total de coincidencias se queda en 0, revisa los números de fila)", start_data_row + 1, total_rows); }

    for current_row_idx in start_data_row..total_rows {
        
        let mut filter = doc! {};
        let mut valid_criteria = true;

        for (col_idx, _, rule) in &parsed_match_rules {
            let cell_val = range.get_value((current_row_idx as u32, *col_idx as u32));
            
            if let Some(val) = cell_val {
                let mut str_val = get_string_val(val); 
                
                if str_val.trim().is_empty() {
                    valid_criteria = false;
                } else {
                    // Aplicar formato si la regla lo pide
                    if rule.use_last_8 {
                        str_val = format_id_string(&str_val);
                    } else {
                        str_val = str_val.trim().to_string(); // Trim básico siempre
                    }

                    let regex_pattern = format!("{}", regex::escape(&str_val)); 
                    filter.insert(&rule.mongo_field, doc! { "$regex": regex_pattern, "$options": "i" });
                }
            } else {
                valid_criteria = false;
            }
        }

        if valid_criteria {
            if let Ok(Some(doc)) = main_collection.find_one(filter).await {
                matches_found += 1;

                // Rellenar
                for (target_col, rule) in &parsed_fill_rules {
                    
                    if let Some(val_origin) = doc.get(&rule.mongo_field) {
                        
                        let mut final_value = if rule.is_lookup {
                            // LOOKUP
                            let lookup_coll = db.collection::<Document>(&rule.lookup_coll);
                            let lookup_filter = doc! { "_id": val_origin.clone() };
                            
                            if let Ok(Some(related_doc)) = lookup_coll.find_one(lookup_filter).await {
                                if let Some(res) = related_doc.get(&rule.lookup_target) {
                                    bson_to_string_smart(res)
                                } else { "REL_ERR_FIELD".to_string() }
                            } else {
                                "REL_ERR_ID".to_string()
                            }
                        } else {
                            bson_to_string_smart(val_origin)
                        };

                        // APLICAR FORMATO AL GUARDAR (Si se marcó el checkbox)
                        if rule.apply_format {
                            final_value = format_id_string(&final_value);
                        }

                        worksheet.write_string(current_row_idx as u32, *target_col as u16, final_value)?;
                    }
                }
            }
        }

        if current_row_idx % 20 == 0 || current_row_idx == total_rows - 1 {
             let mut l = log.lock().unwrap(); 
             // Usamos \r para intentar "sobrescribir" visualmente la última línea en algunos sistemas, 
             // o simplemente mostramos el estado actual.
             *l = format!("Progreso: Fila {} / {} | Coincidencias encontradas: {}", current_row_idx + 1, total_rows, matches_found);
        }
    }

    new_workbook.save(&output_path)?;

    // Obtener ruta absoluta para mostrar al usuario
    let abs_path = std::fs::canonicalize(&output_path).unwrap_or(PathBuf::from(&output_path));
    Ok(abs_path.display().to_string())
}

// --- Helpers ---

// Lógica centralizada de limpieza (Mayúsculas + Últimos 8)
fn format_id_string(input: &str) -> String {
    let mut s = input.trim().to_uppercase();
    if s.len() > 8 {
        let start = s.len() - 8;
        s = s[start..].to_string();
    }
    s
}

fn col_letter_to_index(letter: &str) -> anyhow::Result<usize> {
    let letter = letter.trim().to_uppercase();
    if letter.is_empty() { return Err(anyhow::anyhow!("Letra de columna vacía")); }
    
    let mut col_idx: usize = 0;
    for c in letter.chars() {
        if !c.is_alphabetic() { return Err(anyhow::anyhow!("Columna inválida: {}", letter)); }
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
        Data::DateTime(v) => { sheet.write_string(row, col, v.to_string())?; }, 
        Data::DateTimeIso(v) => { sheet.write_string(row, col, v)?; },
        Data::DurationIso(v) => { sheet.write_string(row, col, v)?; },
        _ => {}, 
    };
    Ok(())
}

fn get_string_val(data: &Data) -> String {
    match data {
        Data::Int(v) => v.to_string(),
        Data::Float(v) => if v.fract() == 0.0 { (*v as i64).to_string() } else { v.to_string() },
        Data::String(v) => v.clone(),
        Data::Bool(v) => v.to_string(),
        _ => "".to_string(),
    }
}

fn bson_to_string_smart(val: &Bson) -> String {
    match val {
        Bson::String(s) => s.clone(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => f.to_string(),
        Bson::ObjectId(oid) => oid.to_string(),
        Bson::DateTime(dt) => dt.try_to_rfc3339_string().unwrap_or("Fecha?".to_string()),
        _ => format!("{:?}", val), 
    }
}

fn main() -> eframe::Result<()> {
    eframe::run_native(
        "Excel-Mongo Expert",
        // Tamaño aumentado a 800x800
        eframe::NativeOptions { viewport: eframe::egui::ViewportBuilder::default().with_inner_size([800.0, 800.0]), ..Default::default() },
        Box::new(|_cc| Ok(Box::new(MyApp::default()))),
    )
}