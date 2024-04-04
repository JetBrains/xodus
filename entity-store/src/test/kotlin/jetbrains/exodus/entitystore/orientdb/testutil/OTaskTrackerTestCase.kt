package jetbrains.exodus.entitystore.orientdb.testutil

class OTaskTrackerTestCase(val orientDB: InMemoryOrientDB) {

    val project1 = orientDB.createProject("project1")
    val project2 = orientDB.createProject("project2")
    val project3 = orientDB.createProject("project3")

    val issue1 = orientDB.createIssue("issue1")
    val issue2 = orientDB.createIssue("issue2")
    val issue3 = orientDB.createIssue("issue3")

    val board1 = orientDB.createBoard("board1")
    val board2 = orientDB.createBoard("board2")
    val board3 = orientDB.createBoard("board3")
}