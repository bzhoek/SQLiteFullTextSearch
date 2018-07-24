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
    
    func testMine() {
      let path = NSSearchPathForDirectoriesInDomains(
        .applicationSupportDirectory, .userDomainMask, true
        ).first! + "/" + Bundle.main.bundleIdentifier!
      
      do {
        try FileManager.default.createDirectory(
          atPath: path, withIntermediateDirectories: true, attributes: nil
        )
        
//        let db = try Connection("\(path)/db.sqlite3")
        let db = try Connection()
        
        let users = Table("filenames")
        let id = Expression<Int64>("id")
        let filename = Expression<String>("filename")
        let modified = Expression<Date>("modified")
        let ftsid = Expression<Int64>("verified")
        
        try db.run(users.create { t in
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
        
        try db.run(emails.create(.FTS4(config)))
        let rowid = try db.run(emails.insert(
          subject <- "Just Checking In",
          body <- "Hey, I was just wondering...did you get my last email?"
        ))
        
        try db.run(users.insert(filename <- "README.md", modified <- Date(), ftsid <- rowid))
        
        let alice = emails.filter(rid == rowid)
        try db.run(alice.update(subject <- "alice@me.com"))
        
        let wonderfulEmails = emails.filter(emails.match("wonder*"))
        XCTAssertEqual(1, try! db.scalar(emails.filter(emails.match("wonder*")).count))
        
        for email in try db.prepare(wonderfulEmails) {
          print("RESULT \(email)")
        }
        
      } catch {
        // Handle error
      }
    }

  
  func testOther() {
    let path = NSSearchPathForDirectoriesInDomains(
      .applicationSupportDirectory, .userDomainMask, true
      ).first! + "/" + Bundle.main.bundleIdentifier!
    
    do {
      try FileManager.default.createDirectory(
        atPath: path, withIntermediateDirectories: true, attributes: nil
      )
      
      //        let db = try Connection("\(path)/db.sqlite3")
      let db = try Connection()
      
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

    } catch {
      // Handle error
    }
  }

}
