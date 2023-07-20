module xodus.ycsbbenchmark {
    requires javafx.controls;
    requires javafx.fxml;


    opens xodus.ycsbbenchmark to javafx.fxml;
    exports xodus.ycsbbenchmark;
}
