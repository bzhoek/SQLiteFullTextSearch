import XCTest
import SQLite
@testable import FullTextSearch

class FullTextSearchTests: XCTestCase {

  var db: Connection!
  var folder: URL!

  override func setUp() {
    super.setUp()
    do {
      folder = URL(fileURLWithPath: NSTemporaryDirectory())
      let url = folder.appendingPathComponent("test.sqlite3")
      if FileManager.default.fileExists(atPath: url.path) {
        try FileManager.default.removeItem(at: url)
      }
      print("\(url.path)")
      db = try Connection(url.path)
    } catch {
      XCTFail(error.localizedDescription)
    }
  }

  override func tearDown() {
    super.tearDown()
  }

  private func getConnection() throws -> Connection {
    let path = NSSearchPathForDirectoriesInDomains(.applicationSupportDirectory, .userDomainMask, true).first! + "/" + Bundle.main.bundleIdentifier!

    try FileManager.default.createDirectory(
        atPath: path, withIntermediateDirectories: true, attributes: nil
    )

    print("PATH \(path)")

    let db = try Connection("\(path)/db.sqlite3")
//    let db = try Connection()
    return db
  }

  func update(_ path: String, connection: Connection) throws {
    let filenames = Table("filenames")
    let id = Expression<Int64>("id")
    let filename = Expression<String>("filename")
    let modified = Expression<Date>("modified")
    let ftsid = Expression<Int64>("ftsid")

    try db.run(filenames.create(ifNotExists: true) { t in
      t.column(id, primaryKey: true)
      t.column(filename, unique: true)
      t.column(modified)
      t.column(ftsid)
    })

    let emails = VirtualTable("emails")
    let subject = Expression<String>("subject")
    let body = Expression<String>("body")

    let config = FTS4Config()
        .column(subject)
        .column(body)
        //          .column(body, [.unindexed])
        .languageId("lid")
        .order(.desc)

    try db.run(emails.create(.FTS4(config), ifNotExists: true))

    if let enumerator = FileManager.default.enumerator(atPath: path) {
      while let file = enumerator.nextObject() as? String {
        if file.hasSuffix(".md") {
          print(file)
          let attributes = try FileManager.default.attributesOfItem(atPath: "\(path)/\(file)")
          let lastModified = attributes[.modificationDate] as! Date

          let filter = filenames.filter(filename == file)

          let indexer = { () -> Int64 in
            return try self.db.run(emails.insert(
                subject <- file,
                body <- try String(contentsOfFile: "\(path)/\(file)")
            ))
          }

          if let record = try db.pluck(filter) {
            let thisModified = record[modified]
            let thatId = record[ftsid]
            let order = Calendar.current.compare(thisModified, to: lastModified, toGranularity: .second)

            if order != .orderedSame {
              print("UPDATED \(lastModified) > \(record[modified])")
              let text = try String(contentsOfFile: "\(path)/\(file)")
              try db.run(emails.filter(rowid == thatId).update(
                  subject <- file,
                  body <- text
              ))
              try db.run(filter.update(modified <- lastModified))
            }

            if record[ftsid] == 0 {
              try self.db.run(filter.update(ftsid <- indexer()))
            }
          } else {
            try db.run(filenames.insert(filename <- file, modified <- lastModified, ftsid <- indexer()))
          }
        }
      }
    }

    for record in try db.prepare(filenames) {
      let url = folder.appendingPathComponent(record[filename])
      if !FileManager.default.fileExists(atPath: url.path) {
        print("REMOVED \(url.path)")
        try db.run(emails.filter(rowid == record[ftsid]).delete())
        try db.run(filenames.filter(rowid == record[id]).delete())
      }

    }
  }

  func testMarkdoneFolder() throws {
    let path = "/Users/bas/Dropbox/Markdone"
    try update(path, connection: db)
  }

  func testIndexUpdates() throws {
    let db = try getConnection()
    let emails = VirtualTable("emails")
    XCTAssertEqual(1, try! db.scalar(emails.filter(emails.match("wonder*")).count))
  }

  func testBundle() throws {
    let emails = VirtualTable("emails")

    try "Lorum".write(to: folder.appendingPathComponent("ADDED.md"), atomically: true, encoding: .utf8)
    try "Ipsum".write(to: folder.appendingPathComponent("CHANGED.md"), atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([FileAttributeKey.modificationDate: Date(timeIntervalSinceNow: -1)], ofItemAtPath: folder.appendingPathComponent("CHANGED.md").path)
    try "Dolor".write(to: folder.appendingPathComponent("REMOVED.md"), atomically: true, encoding: .utf8)
    try update(folder.path, connection: db)
    XCTAssertEqual(1, try! db.scalar(emails.filter(emails.match("ipsum")).count))

    try "Sit".write(to: folder.appendingPathComponent("CHANGED.md"), atomically: true, encoding: .utf8)
    try update(folder.path, connection: db)
    XCTAssertEqual(1, try! db.scalar(emails.filter(emails.match("sit")).count))

    try FileManager.default.removeItem(at: folder.appendingPathComponent("REMOVED.md"))
    try update(folder.path, connection: db)
    XCTAssertEqual(0, try! db.scalar(emails.filter(emails.match("dolor")).count), "Removed file contents still found")
  }

  func testWithCustomTokenizer() throws {
    let db = try getConnection()

    let emails = VirtualTable("emails")
    let subject = Expression<String?>("subject")
    let body = Expression<String?>("body")

    let locale = CFLocaleCopyCurrent()
    let tokenizerName = "tokenizer"
    let tokenizer = CFStringTokenizerCreate(nil, "" as CFString, CFRangeMake(0, 0), UInt(kCFStringTokenizerUnitWord), locale)
    try! db.registerTokenizer(tokenizerName) { string in
      CFStringTokenizerSetString(tokenizer, string as CFString, CFRangeMake(0, CFStringGetLength(string as CFString)))
      if CFStringTokenizerAdvanceToNextToken(tokenizer).isEmpty {
        return nil
      }
      let range = CFStringTokenizerGetCurrentTokenRange(tokenizer)
      let input = CFStringCreateWithSubstring(kCFAllocatorDefault, string as CFString, range)!
      let token = CFStringCreateMutableCopy(nil, range.length, input)!
      CFStringLowercase(token, locale)
      CFStringTransform(token, nil, kCFStringTransformStripDiacritics, false)
      return (token as String, string.range(of: input as String)!)
    }

    try! db.run(emails.create(.FTS4([subject, body], tokenize: .Custom(tokenizerName))))
//      AssertSQL("CREATE VIRTUAL TABLE \"emails\" USING fts4(\"subject\", \"body\", tokenize=\"SQLite.swift\" \"tokenizer\")")

    try! _ = db.run(emails.insert(subject <- "Aún más cáfe!"))
    XCTAssertEqual(1, try! db.scalar(emails.filter(emails.match("aun")).count))
  }

}
