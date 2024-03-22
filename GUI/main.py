import sys

from PyQt5.QtWidgets import (
    QLabel, QApplication, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QWidget, QFormLayout, QFileDialog
)


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        # Class Level UI Elements
        self.base_form_layout = None
        self.root_path_browse_button = None
        self.root_path_line_edit = None
        self.base_layout = None

        # Class Level Data Variables
        self.root_path = None

        # Main window properties
        self.setWindowTitle("RawCooked Executor")
        self.setGeometry(0, 0, 800, 600)

        self.inti_ui()

    def inti_ui(self):
        # Base Layouts
        self.base_layout = QVBoxLayout()
        self.base_form_layout = QFormLayout()

        # Browse Root Path UI
        hbox = QHBoxLayout()
        self.root_path_line_edit = QLineEdit()
        self.root_path_line_edit.setReadOnly(True)
        root_path_label = QLabel("Path to Working Directory: ")
        self.root_path_browse_button = QPushButton("Browse")
        self.root_path_browse_button.clicked.connect(self.launch_root_path_browse_window)
        hbox.addWidget(self.root_path_line_edit)
        hbox.addWidget(self.root_path_browse_button)
        self.base_form_layout.addRow(root_path_label, hbox)
        self.base_layout.addLayout(self.base_form_layout)

        self.setLayout(self.base_layout)

    def launch_root_path_browse_window(self):
        response = QFileDialog.getExistingDirectory(
            self,
            "Select a Folder",
            "/home",
            QFileDialog.ShowDirsOnly | QFileDialog.DontResolveSymlinks,
        )
        self.root_path = response


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()

    sys.exit(app.exec_())
