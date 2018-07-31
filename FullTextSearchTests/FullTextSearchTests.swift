import XCTest
import SQLite
@testable import FullTextSearch

class Indexer {
  let connection: Connection!

  let filenames = Table("filenames")
  let id = Expression<Int64>("id")
  let filename = Expression<String>("filename")
  let modified = Expression<Date>("modified")
  let ftsid = Expression<Int64>("ftsid")

  let contents = VirtualTable("contents")
  let subject = Expression<String>("subject")
  let body = Expression<String>("body")

  init(_ connection: Connection) throws {
    self.connection = connection

    try connection.run(filenames.create(ifNotExists: true) { t in
      t.column(id, primaryKey: true)
      t.column(filename, unique: true)
      t.column(modified)
      t.column(ftsid)
    })

    let config = FTS4Config()
        .column(subject)
        .column(body)
        .languageId("lid")
        .order(.desc)

    try connection.run(contents.create(.FTS4(config), ifNotExists: true))
  }

  func update(_ path: String) throws {
    if let enumerator = FileManager.default.enumerator(atPath: path) {
      while let file = enumerator.nextObject() as? String {
        if file.hasSuffix(".md") {
          print(file)
          let attributes = try FileManager.default.attributesOfItem(atPath: "\(path)/\(file)")
          let lastModified = attributes[.modificationDate] as! Date

          let filter = filenames.filter(filename == file)

          let indexer = { () -> Int64 in
            return try self.connection.run(self.contents.insert(
                self.subject <- file,
                self.body <- try String(contentsOfFile: "\(path)/\(file)")
            ))
          }

          if let record = try connection.pluck(filter) {
            let thisModified = record[modified]
            let thatId = record[ftsid]
            let order = Calendar.current.compare(thisModified, to: lastModified, toGranularity: .second)

            if order != .orderedSame {
              print("UPDATED \(lastModified) > \(record[modified])")
              let text = try String(contentsOfFile: "\(path)/\(file)")
              try connection.run(contents.filter(rowid == thatId).update(
                  self.subject <- file,
                  self.body <- text
              ))
              try connection.run(filter.update(modified <- lastModified))
            }

            if record[ftsid] == 0 {
              try self.connection.run(filter.update(ftsid <- indexer()))
            }
          } else {
            try connection.run(filenames.insert(filename <- file, modified <- lastModified, ftsid <- indexer()))
          }
        }
      }
    }

    for record in try connection.prepare(filenames) {
      let url = URL(fileURLWithPath: path).appendingPathComponent(record[filename])
      if !FileManager.default.fileExists(atPath: url.path) {
        print("REMOVED \(url.path)")
        try connection.run(contents.filter(rowid == record[ftsid]).delete())
        try connection.run(filenames.filter(rowid == record[id]).delete())
      }
    }
  }

}

class FullTextSearchTests: XCTestCase {

  var folder: URL!
  var indexer: Indexer!

  override func setUp() {
    super.setUp()
    do {
      folder = URL(fileURLWithPath: NSTemporaryDirectory())
      let url = folder.appendingPathComponent("test.sqlite3")
      if FileManager.default.fileExists(atPath: url.path) {
        try FileManager.default.removeItem(at: url)
      }
      print("\(url.path)")
      indexer = try Indexer(try Connection(url.path))
    } catch {
      XCTFail(error.localizedDescription)
    }
  }

  override func tearDown() {
    super.tearDown()
  }

  func testMarkdoneFolder() throws {
    let path = "/Users/bas/Dropbox/Markdone"
    try indexer.update(path)
  }

  func testAddChangeDelete() throws {
    try "Lorum".write(to: folder.appendingPathComponent("ADDED.md"), atomically: true, encoding: .utf8)
    try "Ipsum".write(to: folder.appendingPathComponent("CHANGED.md"), atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([FileAttributeKey.modificationDate: Date(timeIntervalSinceNow: -1)], ofItemAtPath: folder.appendingPathComponent("CHANGED.md").path)
    try "Dolor".write(to: folder.appendingPathComponent("REMOVED.md"), atomically: true, encoding: .utf8)
    try indexer.update(folder.path)
    XCTAssertEqual(1, try! indexer.connection.scalar(indexer.contents.filter(indexer.contents.match("ipsum")).count))

    try "Sit".write(to: folder.appendingPathComponent("CHANGED.md"), atomically: true, encoding: .utf8)
    try indexer.update(folder.path)
    XCTAssertEqual(1, try! indexer.connection.scalar(indexer.contents.filter(indexer.contents.match("sit")).count))

    try FileManager.default.removeItem(at: folder.appendingPathComponent("REMOVED.md"))
    try indexer.update(folder.path)
    XCTAssertEqual(0, try! indexer.connection.scalar(indexer.contents.filter(indexer.contents.match("dolor")).count), "Removed file contents still found")
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
