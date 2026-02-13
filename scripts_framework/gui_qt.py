
import sys
import os
import json
from PyQt5 import QtWidgets, QtGui, QtCore

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

class SchedulerTable(QtWidgets.QTableWidget):
    def __init__(self, parent=None):
        super().__init__(0, 4, parent)
        self.setHorizontalHeaderLabels(["Module", "Enabled", "Schedule", "Status"])
        self.horizontalHeader().setStretchLastSection(True)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

    def update_data(self, config_data, statuses):
        self.setRowCount(0)
        for module, settings in config_data["enabled_modules"].items():
            row = self.rowCount()
            self.insertRow(row)
            self.setItem(row, 0, QtWidgets.QTableWidgetItem(module))
            self.setItem(row, 1, QtWidgets.QTableWidgetItem("Yes" if settings["enabled"] else "No"))
            self.setItem(row, 2, QtWidgets.QTableWidgetItem(settings["schedule"]))
            status_item = QtWidgets.QTableWidgetItem(statuses.get(module, "N/A"))

            # Színezés státusz alapján
            if "🚀" in status_item.text() or "Running" in status_item.text():
                status_item.setBackground(QtGui.QColor("#b7ffb7"))
            elif "⏳" in status_item.text() or "Waiting" in status_item.text():
                status_item.setBackground(QtGui.QColor("#b7d0ff"))
            elif "❌" in status_item.text() or "Error" in status_item.text():
                status_item.setBackground(QtGui.QColor("#ffb7b7"))
            elif "Disabled" in status_item.text():
                status_item.setBackground(QtGui.QColor("#e0e0e0"))

            self.setItem(row, 3, status_item)

class SchedulerGUI(QtWidgets.QMainWindow):
    def __init__(self, config_data, status_callback, save_callback):
        super().__init__()
        self.setWindowTitle("Scheduler Monitor (PyQt5)")
        self.resize(800, 600)
        self.config_data = config_data
        self.status_callback = status_callback
        self.save_callback = save_callback

        self.table = SchedulerTable()
        self.setCentralWidget(self.table)

        # Toolbar gombok
        toolbar = self.addToolBar("Actions")
        act_toggle = QtWidgets.QAction("Toggle Enable", self)
        act_edit = QtWidgets.QAction("Edit Schedule/Days", self)
        act_save = QtWidgets.QAction("Save Config", self)

        act_toggle.triggered.connect(self.toggle_selected)
        act_edit.triggered.connect(self.edit_selected)
        act_save.triggered.connect(self.save_config)

        toolbar.addAction(act_toggle)
        toolbar.addAction(act_edit)
        toolbar.addAction(act_save)

        # Timer az állapotfrissítésre
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.refresh_table)
        self.timer.start(5000)

        self.refresh_table()

    def refresh_table(self):
        statuses = self.status_callback()
        self.table.update_data(self.config_data, statuses)

    def get_selected_module(self):
        sel = self.table.selectionModel().selectedRows()
        if not sel:
            return None
        row = sel[0].row()
        module_name = self.table.item(row, 0).text()
        return module_name

    def toggle_selected(self):
        mod = self.get_selected_module()
        if not mod: return
        settings = self.config_data["enabled_modules"][mod]
        settings["enabled"] = not settings["enabled"]

    def edit_selected(self):
        mod = self.get_selected_module()
        if not mod: return
        settings = self.config_data["enabled_modules"][mod]

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle(f"Edit {mod}")
        layout = QtWidgets.QFormLayout(dlg)

        sch_edit = QtWidgets.QLineEdit(settings["schedule"])
        days_edit = QtWidgets.QLineEdit(",".join(settings["days"]))
        layout.addRow("Schedule (HH:MM):", sch_edit)
        layout.addRow("Days (comma separated):", days_edit)

        btn_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        layout.addWidget(btn_box)
        btn_box.accepted.connect(lambda: self.apply_changes(mod, sch_edit.text(), days_edit.text(), dlg))
        btn_box.rejected.connect(dlg.reject)

        dlg.exec_()

    def apply_changes(self, mod, schedule, days_str, dlg):
        settings = self.config_data["enabled_modules"][mod]
        settings["schedule"] = schedule.strip()
        settings["days"] = [d.strip() for d in days_str.split(",") if d.strip()]
        dlg.accept()

    def save_config(self):
        self.save_callback(self.config_data)
        QtWidgets.QMessageBox.information(self, "Saved", "Configuration has been saved!")

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_config(config_data):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)

# Teszt futtatás külön
if __name__ == "__main__":
    def dummy_status():
        return {m: ("🚀 Running" if s["enabled"] else "Disabled") for m, s in config["enabled_modules"].items()}

    config = load_config()
    app = QtWidgets.QApplication(sys.argv)
    gui = SchedulerGUI(config, dummy_status, save_config)
    gui.show()
    sys.exit(app.exec_())
