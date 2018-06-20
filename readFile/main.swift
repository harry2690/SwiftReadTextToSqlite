import SQLite3
import Foundation

func process(string: String, db: OpaquePointer?) throws{
    let lines = string.split(separator: "\n")
    var runIndex = 0
    for line in lines {
        var song_num = "";
        var song_lang = "";
        var song_str_count = "";
        var song_name = "";
        let columns = line.split(separator: "=", omittingEmptySubsequences: false)
        
        if columns.indices.contains(0) {
            song_num = String(columns[0]).trimmingCharacters(in: .whitespaces)
        }
        
        if columns.indices.contains(1) {
            let songInfo = String(columns[1]).trimmingCharacters(in: .whitespaces).split(separator: "\\", omittingEmptySubsequences: false)
            
            if songInfo.indices.contains(1) {
                song_lang = String(songInfo[1]).trimmingCharacters(in: .whitespaces)
            }
            
            if songInfo.indices.contains(2) {
                song_str_count = String(songInfo[2]).trimmingCharacters(in: .whitespaces)
            }
            
            if songInfo.indices.contains(3) {
                let songNameAndType = String(songInfo[3]).trimmingCharacters(in: .whitespaces).split(separator: ".", omittingEmptySubsequences: false)
                
                if songNameAndType.indices.contains(0) {
                    song_name = String(songNameAndType[0]).trimmingCharacters(in: .whitespaces)
                }
            }
//            runIndex = runIndex + 1
//
//            if runIndex == 100 {
//                break
//            }
        }
        
        if song_num != nil && song_num != "" && song_lang != nil && song_lang != "" && song_str_count != nil && song_str_count != "" && song_name != nil && song_name != ""{
                insert(db: db, num: song_num, lang: song_lang, strCount: song_str_count, name: song_name)
        }
    }
}

func processFile(at url: URL, db: OpaquePointer?) throws {
    let s = try String(contentsOf: url)
    try process(string: s, db: db)
//    let jsonStr = json(from: datas)
//    print(jsonStr)
}

func json(from object:Any) -> String? {
    guard let data = try? JSONSerialization.data(withJSONObject: object, options: []) else {
        return nil
    }
    return String(data: data, encoding: String.Encoding.utf8)
}

func openDatabase(part1DbPath: String) -> OpaquePointer? {
    var db: OpaquePointer? = nil
    if sqlite3_open(part1DbPath, &db) == SQLITE_OK {
        print("Successfully opened connection to database at \(part1DbPath)")
    } else {
        print("Unable to open database. Verify that you created the directory described " +
            "in the Getting Started section.")
    }
    return db
}

func createTable(db: OpaquePointer?) {
    let createTableString = """
CREATE TABLE `Song` (
    `Id`    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    `song_num`    INTEGER,
    `song_lang`    TEXT,
    `str_count`    TEXT,
    `song_name`    TEXT
);
"""
    
    // 1
    var createTableStatement: OpaquePointer? = nil
    // 2
    if sqlite3_prepare_v2(db, createTableString, -1, &createTableStatement, nil) == SQLITE_OK {
        // 3
        if sqlite3_step(createTableStatement) == SQLITE_DONE {
            print("Song table created.")
        } else {
            print("Song table could not be created.")
        }
    } else {
        print("CREATE TABLE statement could not be prepared.")
    }
    // 4
    sqlite3_finalize(createTableStatement)
}

func insert(db: OpaquePointer?, num: String?, lang: String?, strCount: String?, name: String?) {
    var insertStatement: OpaquePointer? = nil
    
    var insertStatementString = "INSERT INTO Song (song_num, song_name) VALUES (?, ?)".cString(using: .utf8)!
    
    // 1
    if sqlite3_prepare_v2(db, insertStatementString, -1, &insertStatement, nil) == SQLITE_OK {
        // 3
        sqlite3_bind_int(insertStatement, 1, Int32(num!)!)
//        sqlite3_bind_text(insertStatement, 2, lang!.cString(using: .utf8)!, -1, nil)
//        sqlite3_bind_text(insertStatement, 3, strCount!.cString(using: .utf8)!, -1, nil)
        sqlite3_bind_text(insertStatement, 2, name!.cString(using: .utf8)!, -1, nil)
        
        // 4
        if sqlite3_step(insertStatement) != SQLITE_DONE {
            print("Could not insert row.")
        }
    } else {
        print("INSERT statement could not be prepared.")
    }
    
    var updateStatement: OpaquePointer? = nil
    var updateStatementString = "UPDATE Song SET song_lang=? WHERE song_num=?".cString(using: .utf8)!
    
    // 1
    if sqlite3_prepare_v2(db, updateStatementString, -1, &updateStatement, nil) == SQLITE_OK {
        // 3
        sqlite3_bind_int(updateStatement, 2, Int32(num!)!)
        sqlite3_bind_text(updateStatement, 1, lang!.cString(using: .utf8)!, -1, nil)
        //        sqlite3_bind_text(insertStatement, 3, strCount!.cString(using: .utf8)!, -1, nil)
//        sqlite3_bind_text(insertStatement, 2, name!.cString(using: .utf8)!, -1, nil)
        
        // 4
        if sqlite3_step(updateStatement) != SQLITE_DONE {
            print("Could not insert row.")
        }
    } else {
        print("UPDATE statement could not be prepared.")
    }
    
    var updateStatement2: OpaquePointer? = nil
    var updateStatementString2 = "UPDATE Song SET str_count=? WHERE song_num=?".cString(using: .utf8)!
    
    // 1
    if sqlite3_prepare_v2(db, updateStatementString2, -1, &updateStatement2, nil) == SQLITE_OK {
        // 3
        sqlite3_bind_int(updateStatement2, 2, Int32(num!)!)
        sqlite3_bind_text(updateStatement2, 1, strCount!.cString(using: .utf8)!, -1, nil)
        //        sqlite3_bind_text(insertStatement, 3, strCount!.cString(using: .utf8)!, -1, nil)
        //        sqlite3_bind_text(insertStatement, 2, name!.cString(using: .utf8)!, -1, nil)
        
        // 4
        if sqlite3_step(updateStatement2) != SQLITE_DONE {
            print("Could not insert row.")
        }
    } else {
        print("UPDATE2 statement could not be prepared.")
    }
    
    
    // 5
    sqlite3_finalize(insertStatement)
}

func query(db: OpaquePointer?, queryStatementString:String) {
    var queryStatement: OpaquePointer? = nil
    // 1
    if sqlite3_prepare_v2(db, queryStatementString, -1, &queryStatement, nil) == SQLITE_OK {
        // 2
        while (sqlite3_step(queryStatement) == SQLITE_ROW) {
            let id = sqlite3_column_int(queryStatement, 0)
            let queryResultCol1 = sqlite3_column_text(queryStatement, 1)
            let name = String(cString: queryResultCol1!)
            print("Query Result:")
            print("\(id) | \(name)")
        }
    } else {
        print("SELECT statement could not be prepared")
    }
    // 6
    sqlite3_finalize(queryStatement)
}

func main() {
    //    guard CommandLine.arguments.count > 1 else {
    //        print("usage: \(CommandLine.arguments[0]) file...")
    //        return
    //    }
    //    for path in CommandLine.arguments[1...] {
    
    
    
    let db = openDatabase(part1DbPath: "Song.sqlite")
    
    createTable(db: db)
    
    let path = "SONGLISTU8.txt";
    do {
        let u = URL(fileURLWithPath: path)
        try processFile(at: u, db: db)
    } catch {
        print("error processing: \(path): \(error)")
    }
    
    let queryStatementString = "SELECT * FROM Song WHERE Id = 1;"
    query(db: db, queryStatementString: queryStatementString)
    //    }
}

main()
exit(EXIT_SUCCESS)
