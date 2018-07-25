import XCTest
import SQLite
@testable import FullTextSearch

class FullTextSearchTests: XCTestCase {

  override func setUp() {
    super.setUp()
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

  func testUpdatingIndexer() throws {
    let db = try getConnection()

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
    let rid = Expression<Int64>("rowid")
    let subject = Expression<String>("subject")
    let body = Expression<String>("body")

    let config = FTS4Config()
        .column(subject)
        .column(body)
        //          .column(body, [.unindexed])
        .languageId("lid")
        .order(.desc)

    try db.run(emails.create(.FTS4(config), ifNotExists: true))

    let path = "/Users/bas/Dropbox/Markdone"
    if let enumerator = FileManager.default.enumerator(atPath: path) {
      while let file = enumerator.nextObject() as? String {
        if file.hasSuffix(".md") {
          let attributes = try FileManager.default.attributesOfItem(atPath: "\(path)/\(file)")
          let lastModified = attributes[.modificationDate] as! Date

          let filter = filenames.filter(filename == file)
          if let record = try db.pluck(filter) {

            let thisModified = record[modified]
            let order = Calendar.current.compare(thisModified, to: lastModified, toGranularity: .second)

            if order != .orderedSame {
              print("UPDATED \(lastModified) > \(record[modified])")
              let text = try String(contentsOfFile: "\(path)/\(file)")
              try db.run(emails.filter(rowid == ftsid).update(
                  subject <- file,
                  body <- text
              ))
              try db.run(filter.update(modified <- lastModified))
            }

            if record[ftsid] == 0 {
              let text = try String(contentsOfFile: "\(path)/\(file)")
              let rowid = try db.run(emails.insert(
                  subject <- file,
                  body <- text
              ))
              try db.run(filter.update(ftsid <- rowid))
            }
          } else {
            try db.run(filenames.insert(filename <- file, modified <- lastModified, ftsid <- 0))
          }
        }
      }
    }

    let wonderfulEmails = emails.filter(emails.match("wonder*"))
    XCTAssertEqual(1, try! db.scalar(emails.filter(emails.match("wonder*")).count))

    for email in try db.prepare(wonderfulEmails) {
      print("RESULT \(email)")
    }
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
