# main_ui.py

import sys
import os
from PyQt6.QtWidgets import (
    QApplication, QFileDialog, QTableWidgetItem, QWidget, QHBoxLayout, 
    QCheckBox, QMessageBox, QFrame, QLabel, QTableWidget
)
from PyQt6.QtCore import QThread, QObject, pyqtSignal, Qt, QSettings, QPropertyAnimation, QRect
from PyQt6.QtGui import QBrush, QColor

from ui_components import MainWindow
import analyzer_cli

import analyzer_cli
from analyzer_cli import FIELDS_TO_EXCLUDE_FROM_MARTS_ANALYSIS


class MartsAnalysisWorker(QObject):
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, reporting_path, tabular_path, fields_to_analyze):
        super().__init__()
        self.reporting_path = reporting_path
        self.tabular_path = tabular_path
        self.fields_to_analyze = fields_to_analyze

    def run(self):
        try:
            results = analyzer_cli.analyze_marts_optimization(
                self.reporting_path, 
                self.tabular_path, 
                self.fields_to_analyze,
                progress_callback=self.progress.emit
            )
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(f"An error occurred during MARTS analysis: {e}")

class MartsAuditWorker(QObject):
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, marts_path, reporting_path, fields_to_analyze):
        super().__init__()
        self.marts_path = marts_path
        self.reporting_path = reporting_path
        self.fields_to_analyze = fields_to_analyze

    def run(self):
        print("[DEBUG] 5. MartsAuditWorker.run() started in a new thread.")
        try:
            print("[DEBUG] 6. Calling analyzer_cli.analyze_marts_audit...")
            results = analyzer_cli.analyze_marts_audit(
                self.marts_path, 
                self.reporting_path, 
                self.fields_to_analyze,
                progress_callback=self.progress.emit
            )
            print(f"[DEBUG] 8. Analysis finished. Emitting 'finished' signal with {len(results.get('can_comment_in_marts', []))} optimizable fields.")
            self.finished.emit(results)
        except Exception as e:
            print(f"[DEBUG] ERROR! An exception occurred in worker: {e}")
            self.error.emit(f"An error occurred during MARTS AUDIT analysis: {e}")

class AnalysisWorker(QObject):
    finished = pyqtSignal(list, dict)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)
    
    def __init__(self, pbix_paths, tabular_path, dbt_path):
        super().__init__()
        self.pbix_paths = pbix_paths  
        self.tabular_path = tabular_path
        self.dbt_path = dbt_path

    def run(self):
        try:
            ui_results, intermediate_data = analyzer_cli.perform_analysis(
                self.pbix_paths, self.tabular_path, self.dbt_path, progress_callback=self.progress.emit 
            )
            self.finished.emit(ui_results, intermediate_data)
        except Exception as e:
            self.error.emit(f"An error occurred during analysis: {e}")

class AppController:
    def __init__(self, view: MainWindow):
        self.view = view
        self.thread = None
        self.worker = None
        self.intermediate_data = None
        
        self.marts_tab_results = None     
        self.marts_audit_tab_results = None 
        
        self.reporting_summary_data = None
        self.marts_summary_data = None
        self.marts_only_summary_data = None
        
        self.pbix_paths = []
        self.max_pbix_files = 10
        
        self._connect_signals()
        self._load_settings()
        self.view.adjustSize()

    def _filter_table(self, table: QTableWidget, column_indices: list, filter_text: str):
        filter_lower = filter_text.lower()
        
        for row in range(table.rowCount()):
            is_separator = isinstance(table.cellWidget(row, 0), QFrame)
            if is_separator:
                continue
            
            match = False
            if not filter_lower:
                match = True
            else:
                for col_index in column_indices:
                    item = table.item(row, col_index)
                    if item and filter_lower in item.text().lower():
                        match = True
                        break 
            
            table.setRowHidden(row, not match)

        for row in range(table.rowCount()):
            is_separator = isinstance(table.cellWidget(row, 0), QFrame)
            if not is_separator:
                continue

            group_has_visible_rows = False

            for next_row in range(row + 1, table.rowCount()):
                is_next_row_separator = isinstance(table.cellWidget(next_row, 0), QFrame)
                if is_next_row_separator:
                    break 

                if not table.isRowHidden(next_row):
                    group_has_visible_rows = True
                    break
            
            table.setRowHidden(row, not group_has_visible_rows)
                    
    def _on_marts_analysis_finished(self, marts_results):
        self.marts_tab_results = marts_results 
        self._update_marts_table_with_results(self.marts_tab_results)
        
        self.view.marts_progress_bar.hide()
        self.view.run_marts_analysis_btn.setEnabled(True)
        self.view.apply_marts_changes_btn.setEnabled(True)
        
        summary = self.marts_tab_results.get('summary', {})
        self.view.marts_info_label.setText(
            f"✅ Analysis complete!\n"
            f"Can optimize: {summary.get('can_optimize', 0)} fields | "
            f"Blocked: {summary.get('blocked', 0)} fields | "
            f"Errors: {summary.get('errors', 0)} fields"
        )
        self.view.statusBar().showMessage(
            f"MARTS analysis complete: {summary.get('can_optimize', 0)} fields can be optimized."
        )
        
        self._prepare_marts_audit_tab()

    def _on_marts_checkbox_changed(self, row: int, state: int, target_table: QTableWidget):
        try:
            is_optimizable_item = target_table.item(row, 3)
            if not is_optimizable_item:
                return
                
            was_originally_optimizable = is_optimizable_item.text() == "✅"

            if was_originally_optimizable and state == Qt.CheckState.Unchecked.value:
                background_brush = QBrush(QColor(0, 150, 0, 80))  
            else:
                background_brush = QBrush()

            for col in range(target_table.columnCount()):
                item = target_table.item(row, col)
                original_background = is_optimizable_item.background() if is_optimizable_item.text() != "✅" else QBrush()
                
                if item:
                    final_brush = background_brush if background_brush != QBrush() else original_background
                    item.setBackground(final_brush)
                        
        except Exception as e:
            print(f"Error changing MARTS row color: {e}")

    def _run_marts_audit_analysis(self):
        print("\n[DEBUG] 1. _run_marts_audit_analysis triggered.") 
        fields_to_analyze = []
        for row in range(self.view.marts_audit_table.rowCount()):
            if not self.view.marts_audit_table.cellWidget(row, 0):
                continue
            field_item = self.view.marts_audit_table.item(row, 1)
            if field_item:
                fields_to_analyze.append(field_item.text())
        
        print(f"[DEBUG] 2. Fields to analyze: {len(fields_to_analyze)}")
        if not fields_to_analyze:
            QMessageBox.warning(self.view, "Warning", "No fields to analyze in MARTS AUDIT tab")
            return
        
        self.view.run_marts_audit_analysis_btn.setEnabled(False)
        self.view.apply_marts_audit_changes_btn.setEnabled(False)
        self.view.marts_audit_progress_bar.setValue(0)
        self.view.marts_audit_progress_bar.show()
        
        reporting_path = self.view.dbt_path_input.text()
        marts_path = analyzer_cli.get_marts_path_from_reporting(reporting_path)
        
        print("[DEBUG] 3. Creating MartsAuditWorker.")
        self.thread = QThread()
        self.worker = MartsAuditWorker(marts_path, reporting_path, fields_to_analyze)
        self.worker.moveToThread(self.thread)
        
        self.worker.progress.connect(self.view.marts_audit_progress_bar.setValue)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self._on_marts_audit_analysis_finished)
        self.worker.error.connect(self._on_marts_analysis_error)
        
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(self._clear_thread_references)
        
        print("[DEBUG] 4. Starting thread.")
        self.thread.start()

    def _on_marts_audit_analysis_finished(self, marts_results):
        self.marts_audit_tab_results = marts_results 
        
        self._update_marts_table_with_results(self.marts_audit_tab_results, target_table=self.view.marts_audit_table)
        
        self.view.marts_audit_progress_bar.hide()
        self.view.run_marts_audit_analysis_btn.setEnabled(True)
        self.view.apply_marts_audit_changes_btn.setEnabled(True)
        
        summary = self.marts_audit_tab_results.get('summary', {})
        self.view.marts_audit_info_label.setText(
            f"✅ Audit analysis complete!\n"
            f"Can optimize: {summary.get('can_optimize', 0)} fields | "
            f"Blocked: {summary.get('blocked', 0)} fields | "
            f"Errors: {summary.get('errors', 0)} fields"
        )
        self.view.statusBar().showMessage(
            f"MARTS audit complete: {summary.get('can_optimize', 0)} fields can be optimized."
        )

    def _apply_marts_audit_changes(self):
        if not self.marts_audit_tab_results: # <-- Czytaj z poprawnej zmiennej
            QMessageBox.warning(self.view, "Warning", "Please run MARTS audit analysis first.")
            return
        
        fields_to_comment = []
        for row in range(self.view.marts_audit_table.rowCount()):
            if not self.view.marts_audit_table.cellWidget(row, 0):
                continue
            checkbox_widget = self.view.marts_audit_table.cellWidget(row, 0)
            if checkbox_widget:
                checkbox = checkbox_widget.findChild(QCheckBox)
                if checkbox and checkbox.isChecked() and checkbox.isEnabled():
                    field_item = self.view.marts_audit_table.item(row, 1)
                    if field_item:
                        fields_to_comment.append(field_item.text())
        
        if not fields_to_comment:
            QMessageBox.information(self.view, "Info", "No fields selected for commenting in MARTS audit.")
            return

        total_selected_for_audit = len(fields_to_comment)
        
        reply = QMessageBox.question(
            self.view,
            "Confirm MARTS Audit Changes",
            f"This will comment out {len(fields_to_comment)} fields in your MARTS layer.\n"
            f"This is a deep, internal optimization. Are you sure you want to proceed?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                marts_path = self.marts_audit_tab_results['marts_path']
                
                results = analyzer_cli.comment_out_fields_in_marts_audit(
                    marts_path,
                    fields_to_comment
                )
                
                self.marts_only_summary_data = results
                self.marts_only_summary_data['total_processed'] = total_selected_for_audit
                
                self.view.show_full_summary_btn.show()
                self.view.show_full_summary_btn.setEnabled(True)
                
                QMessageBox.information(
                    self.view,
                    "MARTS Audit Optimization Complete",
                    f"✅ Successfully commented {results['commented_count']} fields in MARTS\n"
                    f"❌ Failed: {results['failed_count']} fields\n\n"
                    f"{results['summary']}\n\n"
                    "You can now generate the full optimization summary."
                )
                
                self.view.statusBar().showMessage(
                    f"MARTS audit optimization complete: {results['commented_count']} fields commented"
                )
                
            except Exception as e:
                QMessageBox.critical(
                    self.view,
                    "Error",
                    f"Failed to apply MARTS audit changes:\n{str(e)}"
                )


    def _on_marts_analysis_error(self, error_msg):
        self.view.marts_progress_bar.hide()
        QMessageBox.critical(self.view, "MARTS Analysis Error", error_msg)
        self.view.run_marts_analysis_btn.setEnabled(True)
        self.view.statusBar().showMessage("MARTS analysis failed.")

    def _connect_signals(self):
        self.view.pbix_browse_btn.clicked.connect(self._browse_pbix_file)
        self.view.tabular_browse_btn.clicked.connect(self._browse_tabular_folder)
        self.view.dbt_browse_btn.clicked.connect(self._browse_dbt_folder)
        self.view.run_analysis_btn.clicked.connect(self._run_analysis)

        QApplication.instance().aboutToQuit.connect(self._save_settings)
        self.view.show_full_summary_btn.clicked.connect(self._show_full_optimization_summary)

        self.view.apply_changes_btn.clicked.connect(self._apply_changes)
        self.view.show_summary_btn.clicked.connect(self._show_analysis_summary)
        self.view.enable_live_mode_checkbox.stateChanged.connect(self._toggle_apply_button)
        self.view.reporting_filter_input.textChanged.connect(
            lambda text: self._filter_table(self.view.results_table, [1], text) 
        )

        self.view.run_marts_analysis_btn.clicked.connect(self._run_marts_analysis)
        self.view.apply_marts_changes_btn.clicked.connect(self._apply_marts_changes)
        self.view.marts_filter_input.textChanged.connect(
            lambda text: self._filter_table(self.view.marts_table, [2], text) 
        )

        self.view.run_marts_audit_analysis_btn.clicked.connect(self._run_marts_audit_analysis)
        self.view.apply_marts_audit_changes_btn.clicked.connect(self._apply_marts_audit_changes)
        self.view.marts_audit_filter_input.textChanged.connect(
            lambda text: self._filter_table(self.view.marts_audit_table, [2], text) 
        )
        
        print("[DEBUG] All signals connected successfully, including MARTS AUDIT and corrected table filters.")
        

    def _clear_widget_focus(self):
        focused_widget = QApplication.focusWidget()
        if focused_widget:
            focused_widget.clearFocus()

    def _adjust_window_size(self, expanding=False):
        QApplication.processEvents()
        if expanding:
            screen_height = self.view.screen().availableGeometry().height()
            max_height = int(screen_height * 0.9)
            natural_height = self.view.sizeHint().height()
            target_height = min(natural_height, max_height)
        else:
            target_height = self.view.sizeHint().height()
        current_geometry = self.view.geometry()
        self.animation = QPropertyAnimation(self.view, b"geometry")
        self.animation.setDuration(300)
        self.animation.setStartValue(current_geometry)
        self.animation.setEndValue(QRect(current_geometry.x(), current_geometry.y(), current_geometry.width(), target_height))
        self.animation.finished.connect(self._clear_widget_focus)
        self.animation.start()

    def _reset_to_input_state(self):
        if self.view.tabs.isVisible():
            self.view.tabs.hide()

        self.view.progress_bar.hide()
        self.view.statusBar().showMessage("Paths have changed. Please run analysis again.")
        self.view.enable_live_mode_checkbox.setEnabled(False)
        self.view.enable_live_mode_checkbox.setChecked(False)
        self.view.show_summary_btn.setEnabled(False)

        self.view.marts_table.clearContents()
        self.view.marts_table.setRowCount(0)
        self.view.run_marts_analysis_btn.setEnabled(False)
        self.view.apply_marts_changes_btn.setEnabled(False)
        self.marts_tab_results = None 

        self.view.marts_audit_table.clearContents()
        self.view.marts_audit_table.setRowCount(0)
        self.view.run_marts_audit_analysis_btn.setEnabled(False)
        self.view.apply_marts_audit_changes_btn.setEnabled(False)
        self.view.show_full_summary_btn.hide()
        self.marts_audit_tab_results = None 
        
        self.reporting_summary_data = None
        self.marts_summary_data = None
        self.marts_only_summary_data = None

        self.intermediate_data = None
        self.view.adjustSize()

    def _prepare_marts_audit_tab(self):
        """
        Gathers, FILTERS, SORTS, and populates the third tab.
        Uses a COMPLETE list of technical fields for exclusion.
        """
        print("Preparing MARTS AUDIT tab...")
        
        marts_path = analyzer_cli.get_marts_path_from_reporting(self.view.dbt_path_input.text())
        if not marts_path:
            self.view.marts_audit_info_label.setText("❌ Could not determine MARTS path. Cannot proceed with audit.")
            return
            
        all_marts_fields = analyzer_cli.get_all_fields_from_dbt_path_for_audit(marts_path)
        
        reporting_column_names_to_exclude = set()
        for row in range(self.view.results_table.rowCount()):
            if not self.view.results_table.item(row, 2):
                continue
            column_item = self.view.results_table.item(row, 2)
            if column_item:
                reporting_column_names_to_exclude.add(column_item.text())

        print(f"Excluding {len(reporting_column_names_to_exclude)} fields from initial analysis.")
        
        candidate_fields = []
        
        fields_to_exclude_technical = {'elt_dmr', 'elt_dmr_core', 'elt_dmr_marts', 'tableindicator'}
        technical_suffixes = ['id', 'bk', 'key']
        marts_excluded_lower = {f.lower() for f in FIELDS_TO_EXCLUDE_FROM_MARTS_ANALYSIS}

        for field_data in all_marts_fields:
            field_name = field_data['field']
            field_name_lower = field_name.lower()

            if (field_name_lower in fields_to_exclude_technical or
                field_name_lower in marts_excluded_lower or
                field_name in reporting_column_names_to_exclude or
                "partition" in field_name_lower or
                any(field_name_lower.endswith(s) for s in technical_suffixes)):
                continue
            
            candidate_fields.append(field_data)
            
        print(f"Found {len(candidate_fields)} candidate fields for the audit after filtering.")

        candidate_fields.sort(key=lambda x: (x['source_model'], x['field']))
        
        self.view.marts_audit_table.clearContents()
        self.view.marts_audit_table.setRowCount(0)
        
        last_model_name = None
        current_row = 0
        for field_data in candidate_fields:
            current_model_name = field_data['source_model']
            
            if last_model_name is not None and current_model_name != last_model_name:
                self.view.marts_audit_table.insertRow(current_row)
                separator = QFrame()
                separator.setFrameShape(QFrame.Shape.HLine)
                separator.setFrameShadow(QFrame.Shadow.Sunken)
                self.view.marts_audit_table.setCellWidget(current_row, 0, separator)
                self.view.marts_audit_table.setSpan(current_row, 0, 1, self.view.marts_audit_table.columnCount())
                current_row += 1

            self.view.marts_audit_table.insertRow(current_row)
            
            checkbox_widget = QWidget()
            checkbox_layout = QHBoxLayout(checkbox_widget)
            checkbox = QCheckBox()
            checkbox.setChecked(True)
            checkbox.stateChanged.connect(
                lambda state, r=current_row: self._on_marts_checkbox_changed(r, state, self.view.marts_audit_table)
            )
            
            checkbox_layout.addWidget(checkbox)
            checkbox_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            checkbox_layout.setContentsMargins(0,0,0,0)
            self.view.marts_audit_table.setCellWidget(current_row, 0, checkbox_widget)

            full_name = f"{field_data['source_model']}.{field_data['field']}"
            self.view.marts_audit_table.setItem(current_row, 1, QTableWidgetItem(full_name))
            self.view.marts_audit_table.setItem(current_row, 2, QTableWidgetItem(field_data['source_model']))
            self.view.marts_audit_table.setItem(current_row, 3, QTableWidgetItem("Pending..."))
            self.view.marts_audit_table.setItem(current_row, 4, QTableWidgetItem(""))
            self.view.marts_audit_table.setItem(current_row, 5, QTableWidgetItem(""))

            last_model_name = current_model_name
            current_row += 1

        self.view.marts_audit_info_label.setText(f"📋 Found {len(candidate_fields)} fields for internal audit. Ready to run analysis.")
        self.view.run_marts_audit_analysis_btn.setEnabled(len(candidate_fields) > 0)

    def _save_settings(self):
        settings = QSettings("MyCompany", "PowerBIAnalyzer")
        settings.setValue("paths/pbix_list", self.pbix_paths)
        settings.setValue("paths/tabular", self.view.tabular_path_input.text())
        settings.setValue("paths/dbt", self.view.dbt_path_input.text())

    def _load_settings(self):
        settings = QSettings("MyCompany", "PowerBIAnalyzer")
        
        self.pbix_paths = []
        
        saved_pbix_list = settings.value("paths/pbix_list", [])
        if saved_pbix_list and isinstance(saved_pbix_list, list):
            for path in saved_pbix_list[:self.max_pbix_files]:
                if os.path.exists(path):
                    self.pbix_paths.append(path)
                else:
                    print(f"⚠️ Skipped non-existent file: {os.path.basename(path)}")
        else:
            old_pbix_path = settings.value("paths/pbix", "")
            if old_pbix_path and os.path.exists(old_pbix_path):
                self.pbix_paths = [old_pbix_path]
                print(f"📋 Migrated old PBIX path: {os.path.basename(old_pbix_path)}")
                settings.setValue("paths/pbix_list", self.pbix_paths)
                settings.remove("paths/pbix")
        
        self._update_pbix_display()
        
        self.view.tabular_path_input.setText(settings.value("paths/tabular", ""))
        self.view.dbt_path_input.setText(settings.value("paths/dbt", ""))
        
        self._check_paths_and_enable_button()
        
        if self.pbix_paths:
            print(f"📁 Loaded {len(self.pbix_paths)} PBIX file(s) from settings:")
            for i, path in enumerate(self.pbix_paths, 1):
                print(f"   {i}. {os.path.basename(path)}")
        else:
            print("📁 No valid PBIX files in settings")

    def _check_paths_and_enable_button(self):
        all_paths_set = all([
            len(self.pbix_paths) > 0,
            self.view.tabular_path_input.text(),
            self.view.dbt_path_input.text()
        ])
        self.view.run_analysis_btn.setEnabled(all_paths_set)

    def _browse_pbix_file(self):
        start_dir = os.path.dirname(self.pbix_paths[0]) if self.pbix_paths else ""
        filters = "ZIP Archives (*.zip);;Power BI Files (*.pbix);;All Files (*)"
        
        paths, _ = QFileDialog.getOpenFileNames(
            self.view, 
            "Select Power BI Source File(s) - Max 10 files", 
            start_dir, 
            filters
        )
        
        if paths:
            if len(paths) > self.max_pbix_files:
                QMessageBox.warning(
                    self.view, 
                    "Too Many Files", 
                    f"Maximum {self.max_pbix_files} files allowed. "
                    f"Selected {len(paths)} files. Using first {self.max_pbix_files}."
                )
                paths = paths[:self.max_pbix_files]
            
            self.pbix_paths = paths
            self._update_pbix_display()
            self._check_paths_and_enable_button()
            self._reset_to_input_state()
            
            print(f"📁 Selected {len(self.pbix_paths)} PBIX file(s)")

    def _update_pbix_display(self):
        if not self.pbix_paths:
            self.view.pbix_path_input.setText("")
            self.view.pbix_path_input.setPlaceholderText("Path to your .zip or .pbix file(s)...")
            return
        
        names = [os.path.basename(p) for p in self.pbix_paths]
        
        if len(names) == 1:
            self.view.pbix_path_input.setText(names[0])
        elif len(names) == 2:
            self.view.pbix_path_input.setText(f"{names[0]}, {names[1]}")
        else:
            remaining = len(names) - 2
            self.view.pbix_path_input.setText(f"{names[0]}, {names[1]} (+{remaining} more)")
        
        if len(self.pbix_paths) > 1:
            self.view.pbix_path_input.setPlaceholderText(f"{len(self.pbix_paths)} PBIX files selected")
        
    def _browse_tabular_folder(self):
        start_dir = self.view.tabular_path_input.text()
        path = QFileDialog.getExistingDirectory(self.view, "Select Tabular Model Folder", start_dir)
        if path:
            self.view.tabular_path_input.setText(path)
            if "datasets" not in path.lower():
                QMessageBox.warning(
                    self.view, 
                    "Check Tabular Path", 
                    "Please double check if path to DATASET is correct."
                )
            self._check_paths_and_enable_button()
            self._reset_to_input_state()

    def _browse_dbt_folder(self):
        start_dir = self.view.dbt_path_input.text()
        path = QFileDialog.getExistingDirectory(self.view, "Select DBT Models Folder", start_dir)
        if path:
            self.view.dbt_path_input.setText(path)
            if "reporting" not in path.lower():
                QMessageBox.warning(
                    self.view, 
                    "Check DBT Path", 
                    "Please double check if you selected REPORTING path."
                )
            self._check_paths_and_enable_button()
            self._reset_to_input_state()

    def _run_analysis(self):
        if self.thread is not None and self.thread.isRunning():
            return
        
        print(f"🔍 DEBUG: self.pbix_paths = {self.pbix_paths}")
        print(f"🔍 DEBUG: type = {type(self.pbix_paths)}")
        print(f"🔍 DEBUG: length = {len(self.pbix_paths)}")
        
        if not self.pbix_paths:
            QMessageBox.warning(self.view, "No Files", "Please select PBIX files first.")
            return
        
        self.view.setMaximumHeight(16777215)
        self._reset_to_input_state()
        self.view.progress_bar.show()
        self.view.run_analysis_btn.setEnabled(False)
        self.view.statusBar().showMessage("Analysis in progress, please wait...")
        
        pbix_paths = self.pbix_paths
        tabular_path = self.view.tabular_path_input.text()
        dbt_path = self.view.dbt_path_input.text()
        
        self.thread = QThread()
        self.worker = AnalysisWorker(pbix_paths, tabular_path, dbt_path)
        self.worker.moveToThread(self.thread)
        self.worker.progress.connect(self.view.progress_bar.setValue)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self._on_analysis_finished)
        self.worker.error.connect(self._on_analysis_error)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(self._clear_thread_references)
        
        self.thread.start()
        self._clear_widget_focus()
    
    def _clear_thread_references(self):
        self.thread = None
        self.worker = None

    def _on_analysis_finished(self, ui_results: list, intermediate_data: dict):
        self.view.progress_bar.hide()
        self.intermediate_data = intermediate_data
        self._populate_table(ui_results)

        self.view.tabs.show()
        self.view.tabs.setCurrentIndex(0)

        self.view.statusBar().showMessage(f"Analysis complete. Found {len(ui_results)} columns.")
        self.view.run_analysis_btn.setEnabled(True)
        self.view.enable_live_mode_checkbox.setEnabled(True)
        self.view.show_summary_btn.setEnabled(True)

        self._adjust_window_size(expanding=True)

    def _transfer_to_marts_tab(self, commented_fields: list):
        self.view.marts_table.clearContents()
        self.view.marts_table.setRowCount(0)

        fields_by_table = {}
        for field in commented_fields:
            if '.' in field:
                table_name, column_name = field.split('.', 1)
                if table_name not in fields_by_table:
                    fields_by_table[table_name] = []
                fields_by_table[table_name].append(column_name)

        final_sorted_fields = []
        for table_name in sorted(fields_by_table.keys()):
            columns_to_sort = fields_by_table[table_name]
            dbt_order = self._get_dbt_field_order_for_marts(table_name)
            
            if dbt_order:
                dbt_order_lower = [col.lower() for col in dbt_order]
                columns_to_sort.sort(key=lambda col: dbt_order_lower.index(col.lower()) if col.lower() in dbt_order_lower else 9999)
            else:
                columns_to_sort.sort()
            
            for col in columns_to_sort:
                final_sorted_fields.append(f"{table_name}.{col}")

        last_table_name = None
        current_row = 0
        for field in final_sorted_fields:
            current_table_name = field.split('.', 1)[0]
            
            if last_table_name is not None and current_table_name != last_table_name:
                self.view.marts_table.insertRow(current_row)
                separator = QFrame()
                separator.setFrameShape(QFrame.Shape.HLine)
                separator.setFrameShadow(QFrame.Shadow.Sunken)
                self.view.marts_table.setCellWidget(current_row, 0, separator)
                self.view.marts_table.setSpan(current_row, 0, 1, self.view.marts_table.columnCount())
                current_row += 1

            self.view.marts_table.insertRow(current_row)
            
            checkbox_widget = QWidget()
            checkbox_layout = QHBoxLayout(checkbox_widget)
            checkbox = QCheckBox()
            checkbox.setChecked(True)

            checkbox.stateChanged.connect(
                lambda state, r=current_row: self._on_marts_checkbox_changed(r, state, self.view.marts_table)
            )
            
            checkbox_layout.addWidget(checkbox)
            checkbox_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            checkbox_layout.setContentsMargins(0,0,0,0)
            self.view.marts_table.setCellWidget(current_row, 0, checkbox_widget)
            
            self.view.marts_table.setItem(current_row, 1, QTableWidgetItem(field))
            self.view.marts_table.setItem(current_row, 2, QTableWidgetItem("Pending analysis..."))
            self.view.marts_table.setItem(current_row, 3, QTableWidgetItem("❓"))
            self.view.marts_table.setItem(current_row, 4, QTableWidgetItem(""))
            self.view.marts_table.setItem(current_row, 5, QTableWidgetItem(""))

            last_table_name = current_table_name
            current_row += 1
        
        self.view.run_marts_analysis_btn.setEnabled(True)
        self.view.tabs.setCurrentIndex(1)
        
        self.view.statusBar().showMessage(
            f"Transferred {len(commented_fields)} fields to MARTS tab. Click 'Run Marts Analysis' to check dependencies."
        )
        
        self.view.marts_info_label.setText(
            f"📋 {len(commented_fields)} fields ready for MARTS analysis."
        )

    def _run_marts_analysis(self):
        fields_to_analyze = []
        for row in range(self.view.marts_table.rowCount()):
            if not self.view.marts_table.cellWidget(row, 0):
                continue
            field_item = self.view.marts_table.item(row, 1)
            if field_item:
                fields_to_analyze.append(field_item.text())
        
        if not fields_to_analyze:
            QMessageBox.warning(self.view, "Warning", "No fields to analyze in MARTS tab")
            return
        
        self.view.run_marts_analysis_btn.setEnabled(False)
        self.view.apply_marts_changes_btn.setEnabled(False)
        self.view.marts_progress_bar.setValue(0)
        self.view.marts_progress_bar.show()
        
        reporting_path = self.view.dbt_path_input.text()
        tabular_path = self.view.tabular_path_input.text()
        
        self.thread = QThread()
        self.worker = MartsAnalysisWorker(reporting_path, tabular_path, fields_to_analyze)
        self.worker.moveToThread(self.thread)
        
        self.worker.progress.connect(self.view.marts_progress_bar.setValue)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self._on_marts_analysis_finished)
        self.worker.error.connect(self._on_marts_analysis_error)
        
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(self._clear_thread_references)
        
        self.thread.start()

    def _update_marts_table_with_results(self, marts_results, target_table=None):
        if not marts_results:
            return

        if target_table is None:
            target_table = self.view.marts_table
        
        field_map = {}
        
        for item in marts_results.get('can_comment_in_marts', []):
            field_map[item['field']] = {
                'can_comment': True,
                'source_model': item.get('source_model', 'Unknown'),
                'blocked_by': '',
                'usage_example': ''
            }
        
        for item in marts_results.get('cannot_comment_in_marts', []):
            blocking_files = ', '.join(item.get('blocking_models', [])[:2])
            if len(item.get('blocking_models', [])) > 2:
                blocking_files += f" (+{len(item['blocking_models'])-2} more)"
            
            usage_example = ''
            if item.get('blocking_details'):
                usage_example = item['blocking_details'][0] if item['blocking_details'] else ''
            
            field_map[item['field']] = {
                'can_comment': False,
                'source_model': item.get('source_model', 'Unknown'),
                'blocked_by': blocking_files,
                'usage_example': usage_example
            }
        
        for item in marts_results.get('errors', []):
            field_map[item['field']] = {
                'can_comment': False,
                'source_model': 'ERROR',
                'blocked_by': 'Model not found',
                'usage_example': item.get('error', '')
            }
        
        for row in range(target_table.rowCount()):
            field_item = target_table.item(row, 1)
            if not field_item:
                continue
                
            field_name = field_item.text()
            if field_name in field_map:
                info = field_map[field_name]
                
                target_table.setItem(row, 2, QTableWidgetItem(info['source_model']))
                
                if info['can_comment']:
                    used_item = QTableWidgetItem("✅")
                    used_item.setBackground(QBrush(QColor(0, 150, 0, 50)))
                    
                    checkbox_widget = target_table.cellWidget(row, 0)
                    if checkbox_widget:
                        checkbox = checkbox_widget.findChild(QCheckBox)
                        if checkbox:
                            checkbox.setChecked(True)
                else:
                    used_item = QTableWidgetItem("❌")
                    used_item.setBackground(QBrush(QColor(150, 0, 0, 50)))
                    
                    checkbox_widget = target_table.cellWidget(row, 0)
                    if checkbox_widget:
                        checkbox = checkbox_widget.findChild(QCheckBox)
                        if checkbox:
                            checkbox.setChecked(False)
                            checkbox.setEnabled(False)
                
                target_table.setItem(row, 3, used_item)
                
                blocked_item = QTableWidgetItem(info['blocked_by'])
                if info['blocked_by']:
                    blocked_item.setToolTip(f"Full list: {info['blocked_by']}")
                target_table.setItem(row, 4, blocked_item)
                
                usage_item = QTableWidgetItem(info['usage_example'][:100])
                if info['usage_example']:
                    usage_item.setToolTip(info['usage_example'])
                target_table.setItem(row, 5, usage_item)
        
        target_table.resizeColumnsToContents()

    def _apply_marts_changes(self):
        if not self.marts_tab_results: 
            QMessageBox.warning(self.view, "Warning", "Please run MARTS analysis first.")
            return
        
        fields_to_comment = []
        for row in range(self.view.marts_table.rowCount()):
            checkbox_widget = self.view.marts_table.cellWidget(row, 0)
            if checkbox_widget:
                checkbox = checkbox_widget.findChild(QCheckBox)
                if checkbox and checkbox.isChecked() and checkbox.isEnabled():
                    field_item = self.view.marts_table.item(row, 1)
                    if field_item:
                        fields_to_comment.append(field_item.text())
        
        if not fields_to_comment:
            QMessageBox.information(self.view, "Info", "No fields selected for commenting in MARTS.")
            return
        
        total_selected_for_marts = len(fields_to_comment)
        
        reply = QMessageBox.question(
            self.view,
            "Confirm MARTS Changes",
            f"This will comment out {len(fields_to_comment)} fields in MARTS layer.\n\n"
            f"This is a deeper optimization that affects the source data layer.\n"
            f"Are you sure you want to proceed?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                safe_to_comment = []
                for field_info in self.marts_tab_results.get('can_comment_in_marts', []): 
                    if field_info['field'] in fields_to_comment:
                        safe_to_comment.append(field_info)
                
                filtered_results = {
                    'can_comment_in_marts': safe_to_comment,
                    'marts_path': self.marts_tab_results['marts_path'] 
                }
                
                results = analyzer_cli.comment_out_fields_in_marts(
                    filtered_results,
                    self.intermediate_data.get('tabular_model_path'),
                    self.view.dbt_path_input.text()
                )
                
                self.marts_summary_data = results
                self.marts_summary_data['total_processed'] = total_selected_for_marts
                
                self.view.show_full_summary_btn.show()
                
                QMessageBox.information(
                    self.view,
                    "MARTS Optimization Complete",
                    f"✅ Successfully commented {results['commented_count']} fields in MARTS\n"
                    f"❌ Failed: {results['failed_count']} fields\n\n"
                    f"{results['summary']}"
                )
                
                self.view.statusBar().showMessage(
                    f"MARTS optimization complete: {results['commented_count']} fields commented"
                )
                self.view.tabs.setCurrentIndex(2)
                
            except Exception as e:
                QMessageBox.critical(
                    self.view,
                    "Error",
                    f"Failed to apply MARTS changes:\n{str(e)}"
                )
               

    def _on_analysis_error(self, error_msg: str):
        self.view.progress_bar.hide()
        QMessageBox.critical(self.view, "Analysis Error", error_msg)
        self.view.statusBar().showMessage("Analysis failed. Please try again.")
        self.view.run_analysis_btn.setEnabled(True)
        self.view.adjustSize()

    def _populate_table(self, data: list):
        sorted_data = self._sort_data_by_dbt_order(data)
        
        self.view.results_table.clearContents()
        self.view.results_table.setRowCount(0)
        last_table_name = None
        current_row = 0
        
        for row_data in sorted_data:
            current_table_name = row_data.get("table", "")
            
            if last_table_name is not None and current_table_name != last_table_name:
                self.view.results_table.insertRow(current_row)
                separator = QFrame()
                separator.setFrameShape(QFrame.Shape.HLine)
                separator.setFrameShadow(QFrame.Shadow.Sunken)
                self.view.results_table.setCellWidget(current_row, 0, separator)
                self.view.results_table.setSpan(current_row, 0, 1, self.view.results_table.columnCount())
                current_row += 1
                
            self.view.results_table.insertRow(current_row)
            is_used = any([row_data.get(k) for k in ["visualization", "measure", "indirect_measure", "hierarchy", "filter", "relationship", "tabular_sort", "rls"]])
            
            checkbox_widget = QWidget()
            checkbox_layout = QHBoxLayout(checkbox_widget)
            checkbox = QCheckBox()
            checkbox.setChecked(not is_used)
            
            checkbox.stateChanged.connect(lambda state, row=current_row: self._on_checkbox_changed(row, state))
            
            checkbox_layout.addWidget(checkbox)
            checkbox_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            checkbox_layout.setContentsMargins(0,0,0,0)
            self.view.results_table.setCellWidget(current_row, 0, checkbox_widget)
            self.view.results_table.setItem(current_row, 1, QTableWidgetItem(row_data.get("table", "")))
            self.view.results_table.setItem(current_row, 2, QTableWidgetItem(row_data.get("column", "")))
            self.view.results_table.setItem(current_row, 3, QTableWidgetItem("✅" if is_used else "❌"))
            self.view.results_table.setItem(current_row, 4, QTableWidgetItem("✅" if row_data.get("visualization") else "❌"))
            self.view.results_table.setItem(current_row, 5, QTableWidgetItem("✅" if row_data.get("measure") else "❌"))
            self.view.results_table.setItem(current_row, 6, QTableWidgetItem("✅" if row_data.get("filter") else "❌"))
            self.view.results_table.setItem(current_row, 7, QTableWidgetItem("✅" if row_data.get("indirect_measure") else "❌"))
            self.view.results_table.setItem(current_row, 8, QTableWidgetItem("✅" if row_data.get("relationship") else "❌"))
            self.view.results_table.setItem(current_row, 9, QTableWidgetItem("✅" if row_data.get("hierarchy") else "❌"))
            self.view.results_table.setItem(current_row, 10, QTableWidgetItem("✅" if row_data.get("tabular_sort") else "❌"))
            self.view.results_table.setItem(current_row, 11, QTableWidgetItem("✅" if row_data.get("rls") else "❌"))
            
            last_table_name = current_table_name
            current_row += 1

    def _sort_data_by_dbt_order(self, data: list) -> list:
        if not self.intermediate_data:
            return sorted(data, key=lambda x: (x.get("table", ""), x.get("column", "")))
        
        try:
            tables_data = {}
            for item in data:
                table_name = item.get("table", "")
                if table_name not in tables_data:
                    tables_data[table_name] = []
                tables_data[table_name].append(item)
            
            sorted_result = []
            
            for table_name in sorted(tables_data.keys()):
                table_items = tables_data[table_name]
                
                print(f"📄 Sorting table '{table_name}' with {len(table_items)} columns...")
                
                dbt_order = self._get_dbt_field_order(table_name)
                
                if dbt_order:
                    print(f"   📋 DBT order: {dbt_order[:5]}{'...' if len(dbt_order) > 5 else ''}")
                    
                    def sort_key(item):
                        column_name = item.get("column", "")
                        if column_name in dbt_order:
                            dbt_index = dbt_order.index(column_name)
                            print(f"   🔍 '{column_name}' -> DBT position {dbt_index}")
                            return dbt_index
                        else:
                            unknown_index = 999999 + ord(column_name[0].lower()) if column_name else 999999
                            print(f"   ❓ '{column_name}' -> Unknown, position {unknown_index}")
                            return unknown_index
                    
                    table_items.sort(key=sort_key)
                    
                    print(f"   ✅ Final order for table '{table_name}':")
                    for i, item in enumerate(table_items[:10]):
                        column = item.get("column", "")
                        is_used = any([item.get(k) for k in ["visualization", "measure", "indirect_measure", "hierarchy", "filter", "relationship"]])
                        status = "USED" if is_used else "UNUSED"
                        print(f"      {i+1:2d}. {column} ({status})")
                    if len(table_items) > 10:
                        print(f"      ... and {len(table_items) - 10} more")
                        
                else:
                    print(f"   ⚠️ No DBT order found, using alphabetical fallback")
                    table_items.sort(key=lambda x: x.get("column", ""))
                
                sorted_result.extend(table_items)
            
            print(f"🎯 Total sorted items: {len(sorted_result)}")
            return sorted_result
            
        except Exception as e:
            print(f"⚠️ Warning: Could not sort by DBT order: {e}")
            return sorted(data, key=lambda x: (x.get("table", ""), x.get("column", "")))
            
    def _get_dbt_field_order(self, table_name: str) -> list:
        try:
            tabular_model_path = self.intermediate_data.get("tabular_model_path", "")
            dbt_models_path = self.view.dbt_path_input.text()
            
            if not tabular_model_path or not dbt_models_path:
                print(f"   ❌ Missing paths for table '{table_name}'")
                return []
            
            alias = analyzer_cli.find_snowflake_alias_for_table(table_name, tabular_model_path)
            if not alias:
                print(f"   ❌ No Snowflake alias found for table '{table_name}'")
                return []
            
            print(f"   🔍 Found alias '{alias}' for table '{table_name}'")
            
            dbt_file = analyzer_cli.find_dbt_file_for_alias(alias, dbt_models_path)
            if not dbt_file:
                print(f"   ❌ No DBT file found for alias '{alias}'")
                return []
            
            print(f"   🔍 Found DBT file: {os.path.basename(dbt_file)}")
            
            dbt_columns = analyzer_cli.analyze_dbt_columns_fixed(dbt_file)
            
            field_order = list(dbt_columns.values())
            print(f"   📋 Extracted {len(field_order)} columns from DBT file")
            
            return field_order
            
        except Exception as e:
            print(f"   ❌ Error getting DBT field order for {table_name}: {e}")
            return []

    def _get_dbt_field_order_for_marts(self, table_name: str) -> list:
        try:
            tabular_model_path = self.intermediate_data.get("tabular_model_path", "")
            reporting_path = self.view.dbt_path_input.text()
            
            if 'reporting' not in reporting_path:
                return []
            marts_path = reporting_path.replace('reporting', 'marts')
            if not os.path.exists(marts_path):
                return []

            alias = analyzer_cli.find_snowflake_alias_for_table(table_name, tabular_model_path)
            if not alias:
                return []
            
            dbt_file = analyzer_cli.find_dbt_file_for_alias(alias, marts_path)
            if not dbt_file:
                dbt_file = analyzer_cli.find_dbt_file_for_alias(f"marts_{table_name.lower()}", marts_path)
                if not dbt_file:
                    return []

            dbt_columns = analyzer_cli.analyze_dbt_columns_fixed(dbt_file)
            return list(dbt_columns.values())
            
        except Exception:
            return []

    def _on_checkbox_changed(self, row: int, state: int):
        try:
            table_item = self.view.results_table.item(row, 1)
            column_item = self.view.results_table.item(row, 2)
            is_used_item = self.view.results_table.item(row, 3)
            
            if not all([table_item, column_item, is_used_item]):
                return
                
            is_originally_unused = is_used_item.text() == "❌"
            
            for col in range(self.view.results_table.columnCount()):
                item = self.view.results_table.item(row, col)
                if item:
                    if is_originally_unused and state == Qt.CheckState.Unchecked.value:
                        item.setBackground(QBrush(QColor(0, 150, 0, 80)))
                    else:
                        item.setBackground(QBrush())
                        
        except Exception as e:
            print(f"Error changing row color: {e}")

    def _generate_analysis_summary(self) -> dict:
        summary = {
            'total_columns': 0,
            'used_columns': 0,
            'unused_columns': 0,
            'usage_breakdown': {
                'visualization': 0,
                'measure': 0,
                'filter': 0,
                'indirect_measure': 0,
                'relationship': 0,
                'hierarchy': 0
            },
            'user_decisions': {
                'manually_kept': 0,
                'manually_removed': 0
            },
            'commenting_stats': {
                'to_comment': 0,
                'to_keep': 0
            }
        }
        
        for row in range(self.view.results_table.rowCount()):
            widget = self.view.results_table.cellWidget(row, 0)
            if not isinstance(widget, QWidget) or not widget.findChild(QCheckBox):
                continue
                
            summary['total_columns'] += 1
            
            checkbox = widget.findChild(QCheckBox)
            is_used_item = self.view.results_table.item(row, 3)
            viz_item = self.view.results_table.item(row, 4)
            measure_item = self.view.results_table.item(row, 5)
            filter_item = self.view.results_table.item(row, 6)
            indirect_item = self.view.results_table.item(row, 7)
            relation_item = self.view.results_table.item(row, 8)
            hierarchy_item = self.view.results_table.item(row, 9)
            
            if not is_used_item:
                continue
                
            is_used = is_used_item.text() == "✅"
            is_checked = checkbox.isChecked()
            
            if is_used:
                summary['used_columns'] += 1
            else:
                summary['unused_columns'] += 1
                
            if viz_item and viz_item.text() == "✅":
                summary['usage_breakdown']['visualization'] += 1
            if measure_item and measure_item.text() == "✅":
                summary['usage_breakdown']['measure'] += 1
            if filter_item and filter_item.text() == "✅":
                summary['usage_breakdown']['filter'] += 1
            if indirect_item and indirect_item.text() == "✅":
                summary['usage_breakdown']['indirect_measure'] += 1
            if relation_item and relation_item.text() == "✅":
                summary['usage_breakdown']['relationship'] += 1
            if hierarchy_item and hierarchy_item.text() == "✅":
                summary['usage_breakdown']['hierarchy'] += 1
                
            row_has_green_background = False
            for col in range(self.view.results_table.columnCount()):
                item = self.view.results_table.item(row, col)
                if item and item.background().color().green() > 100:
                    row_has_green_background = True
                    break
                    
            if row_has_green_background:
                summary['user_decisions']['manually_kept'] += 1
            elif is_used and is_checked:
                summary['user_decisions']['manually_removed'] += 1
                
            if is_checked:
                summary['commenting_stats']['to_comment'] += 1
            else:
                summary['commenting_stats']['to_keep'] += 1
        
        return summary

    def _format_summary_message(self, summary: dict, commented_count: int, is_planning: bool = False) -> str:
        total = summary['total_columns']
        used = summary['used_columns']
        unused = summary['unused_columns']
        
        if total == 0:
            return "No data to summarize."
        
        used_pct = (used / total) * 100
        unused_pct = (unused / total) * 100
        
        action_section = "💬 PLANNED COMMENTING:" if is_planning else "💬 COMMENTING RESULTS:"
        comment_text = "Columns to comment out:" if is_planning else "Columns commented out:"
        success_text = "" if is_planning else f"\n• Success rate: {((commented_count / max(summary['commenting_stats']['to_comment'], 1)) * 100):.1f}%"
        
        message = f"""📊 ANALYSIS & COMMENTING SUMMARY

🔍 COLUMN ANALYSIS:
• Total columns analyzed: {total:,}
• Used columns: {used:,} ({used_pct:.1f}%)
• Unused columns: {unused:,} ({unused_pct:.1f}%)

📈 USAGE BREAKDOWN:
• In Visualizations: {summary['usage_breakdown']['visualization']:,}
• In Measures (direct): {summary['usage_breakdown']['measure']:,}
• In Filters: {summary['usage_breakdown']['filter']:,}
• In Measures (indirect): {summary['usage_breakdown']['indirect_measure']:,}
• In Relationships: {summary['usage_breakdown']['relationship']:,}
• In Hierarchies: {summary['usage_breakdown']['hierarchy']:,}

👤 USER DECISIONS:
• Manually kept (unused → keep): {summary['user_decisions']['manually_kept']:,}
• Manually removed (used → comment): {summary['user_decisions']['manually_removed']:,}

{action_section}
• {comment_text} {commented_count:,}
• Columns kept: {summary['commenting_stats']['to_keep']:,}{success_text}

🎯 IMPACT:
• Columns removed from DBT: {commented_count:,} / {total:,} ({(commented_count/total)*100:.1f}%)
• Performance potential improvement: {'High' if commented_count > total * 0.3 else 'Medium' if commented_count > total * 0.1 else 'Low'}
"""
        
        return message

    def _show_analysis_summary(self):
        if not self.intermediate_data:
            QMessageBox.warning(self.view, "Warning", "Please run an analysis first.")
            return
            
        summary = self._generate_analysis_summary()
        planned_to_comment = summary['commenting_stats']['to_comment']
        summary_msg = self._format_summary_message(summary, planned_to_comment, is_planning=True)
        
        dialog = QMessageBox(self.view)
        dialog.setWindowTitle("📊 Analysis Summary")
        dialog.setIcon(QMessageBox.Icon.Information)
        
        dialog.setText(summary_msg)
        
        dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
        
        dialog.setMinimumSize(600, 500)
        dialog.resize(700, 600)
        
        style_sheet = """
            QMessageBox {
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 13px;
            }
            QMessageBox QLabel {
                color: #FFFFFF;
                background-color: #2E2E2E;
                padding: 10px;
                border-radius: 5px;
            }
        """
        dialog.setStyleSheet(style_sheet)
        
        for widget in dialog.findChildren(QWidget):
            if isinstance(widget, QLabel):
                widget.setWordWrap(True)
                widget.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
                widget.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                break
        
        dialog.exec()

    def _toggle_apply_button(self):
        is_checked = self.view.enable_live_mode_checkbox.isChecked()
        self.view.apply_changes_btn.setEnabled(is_checked)

    def _apply_changes(self):
        if not self.intermediate_data:
            QMessageBox.warning(self.view, "Warning", "Please run an analysis first.")
            return
            
        summary = self._generate_analysis_summary()
        
        columns_to_comment = []
        for row in range(self.view.results_table.rowCount()):
            widget = self.view.results_table.cellWidget(row, 0)
            if isinstance(widget, QWidget) and widget.findChild(QCheckBox):
                checkbox = widget.findChild(QCheckBox)
                if checkbox.isChecked():
                    table_item = self.view.results_table.item(row, 1)
                    column_item = self.view.results_table.item(row, 2)
                    if table_item and column_item:
                        table = table_item.text()
                        column = column_item.text()
                        columns_to_comment.append(f"{table}.{column}")
                        
        if not columns_to_comment:
            summary_msg = self._format_summary_message(summary, 0, is_planning=True)
            QMessageBox.information(self.view, "Analysis Summary", summary_msg)
            return
            
        reply = QMessageBox.question(
            self.view, 
            "Confirm Changes", 
            f"This will comment out {len(columns_to_comment)} columns in your DBT project.\n"
            f"This action cannot be undone from the app.\n\n"
            f"Are you sure you want to proceed?", 
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, 
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.intermediate_data['columns_to_comment_out'] = columns_to_comment
            dbt_path = self.view.dbt_path_input.text()
            
            try:
                analyzer_cli.apply_changes(dbt_path, self.intermediate_data)
                
                error_report = analyzer_cli.generate_error_report()

                fields_to_exclude_technical = {'elt_dmr_core', 'elt_dmr_marts'}
                marts_excluded_lower = {f.lower() for f in FIELDS_TO_EXCLUDE_FROM_MARTS_ANALYSIS}

                def filter_fields_for_marts(field_list):
                    final_list = []
                    for field in field_list:
                        field_name = field.split('.')[-1].lower()
                        if field_name not in fields_to_exclude_technical and field_name not in marts_excluded_lower:
                            final_list.append(field)
                    return final_list

                if error_report['has_errors']:
                    self._show_commenting_errors(error_report, len(columns_to_comment))
                    
                    failed_tables = set(error_report.get('failed_tables', []))
                    successfully_commented_columns = [
                        col for col in columns_to_comment 
                        if col.split('.', 1)[0] not in failed_tables
                    ]
                    
                    transferable_columns = filter_fields_for_marts(successfully_commented_columns)

                    if transferable_columns:
                        self.reporting_summary_data = {
                            'summary': summary, 
                            'commented_count': len(successfully_commented_columns)
                        }
                        self.view.statusBar().showMessage(
                            f"✅ Partial success. Transferring {len(transferable_columns)} columns to MARTS tab."
                        )
                        self._transfer_to_marts_tab(transferable_columns)
                    else:
                        self.view.statusBar().showMessage(
                            "❌ Commenting failed or only technical fields remaining. No columns to transfer."
                        )
                        
                else:
                    commented_count = len(columns_to_comment)
                    self.reporting_summary_data = {'summary': summary, 'commented_count': commented_count}
                    summary_msg = self._format_summary_message(summary, commented_count, is_planning=False)
                    self._show_success_dialog(summary_msg)
                
                    transferable_columns = filter_fields_for_marts(columns_to_comment)

                    self.view.statusBar().showMessage(
                        f"✅ Commenting completed. Transferring {len(transferable_columns)} columns to MARTS tab."
                    )
                    self._transfer_to_marts_tab(transferable_columns)
                
            except Exception as e:
                QMessageBox.critical(
                    self.view, 
                    "Error", 
                    f"An error occurred during commenting:\n{str(e)}"
                )
                self.view.statusBar().showMessage("❌ Commenting failed. Check logs for details.")

    def _show_commenting_errors(self, error_report: dict, total_requested: int):
        successful_count = total_requested - error_report['total_affected_columns']
        failed_tables = error_report['failed_tables']
        
        main_message = f"""⚠️ PARTIAL SUCCESS - Some columns could not be commented out

        📊 SUMMARY:
        • Total columns requested: {total_requested}
        • Successfully commented: {successful_count}
        • Failed to comment: {error_report['total_affected_columns']}
        • Tables with errors: {error_report['total_errors']}

        ❌ FAILED TABLES:
        {chr(10).join(f"• {table}" for table in failed_tables[:10])}
        {f"... and {len(failed_tables) - 10} more" if len(failed_tables) > 10 else ""}

🔍 ERROR BREAKDOWN:"""
        
        for error_type, count in error_report['error_types'].items():
            description = error_report['error_descriptions'].get(error_type, error_type)
            main_message += f"\n• {description}: {count} table(s)"
        
        main_message += f"""

💡 WHAT TO DO:
1. Check the terminal/console log for detailed error messages
2. Fix the underlying issues (missing DBT files, parsing problems)
3. Re-run the commenting for failed tables
4. Check Git changes - successful comments were applied

⚠️ Note: Successfully commented columns are already updated in your DBT files."""
        
        error_dialog = QMessageBox(self.view)
        error_dialog.setWindowTitle("⚠️ Commenting Partially Failed")
        error_dialog.setIcon(QMessageBox.Icon.Warning)
        error_dialog.setText(main_message)
        
        detailed_info = "DETAILED ERROR LOG:\n\n"
        for i, error in enumerate(error_report['detailed_errors'][:5], 1):
            detailed_info += f"{i}. Table: {error['table']}\n"
            detailed_info += f"   Error: {error['error_message']}\n"
            detailed_info += f"   Columns affected: {error['columns_affected']}\n"
            if 'snowflake_alias' in error:
                detailed_info += f"   Snowflake alias: {error['snowflake_alias']}\n"
            if 'dbt_file' in error:
                detailed_info += f"   DBT file: {error['dbt_file']}\n"
            detailed_info += "\n"
        
        if len(error_report['detailed_errors']) > 5:
            detailed_info += f"... and {len(error_report['detailed_errors']) - 5} more errors. Check terminal for full details."
        
        error_dialog.setDetailedText(detailed_info)
        error_dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
        
        error_dialog.setMinimumSize(700, 600)
        error_dialog.resize(800, 700)
        
        error_style_sheet = """
            QMessageBox {
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 13px;
            }
            QMessageBox QLabel {
                color: #FFFFFF;
                background-color: #2E2E2E;
                padding: 10px;
                border-radius: 5px;
            }
        """
        error_dialog.setStyleSheet(error_style_sheet)
        
        for widget in error_dialog.findChildren(QWidget):
            if isinstance(widget, QLabel):
                widget.setWordWrap(True)
                widget.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
                widget.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                break
        
        error_dialog.exec()

    def _show_success_dialog(self, summary_msg: str):
        dialog = QMessageBox(self.view)
        dialog.setWindowTitle("✅ Commenting Completed - Full Summary")
        dialog.setIcon(QMessageBox.Icon.Information)
        dialog.setText(summary_msg)
        dialog.setInformativeText("Commenting process completed successfully! Check Git for file changes.")
        dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
        dialog.setMinimumSize(650, 550)
        dialog.resize(750, 650)
        
        completion_style_sheet = """
            QMessageBox {
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 13px;
            }
            QMessageBox QLabel {
                color: #FFFFFF;
                background-color: #2E2E2E;
                padding: 10px;
                border-radius: 5px;
            }
        """
        dialog.setStyleSheet(completion_style_sheet)
        
        for widget in dialog.findChildren(QWidget):
            if isinstance(widget, QLabel):
                widget.setWordWrap(True)
                widget.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
                widget.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                break
        
        dialog.exec()

    def _show_full_optimization_summary(self):
        """Displays the final, multi-stage optimization summary."""
        if not self.reporting_summary_data:
            QMessageBox.warning(self.view, "No Data", "At least the REPORTING optimization step must be completed.")
            return

        summary_msg = self._format_full_summary_message()
        
        dialog = QMessageBox(self.view)
        dialog.setWindowTitle("🏆 Full Optimization Summary")
        dialog.setIcon(QMessageBox.Icon.Information)
        dialog.setText(summary_msg)
        dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
        dialog.setMinimumSize(700, 600)
        dialog.resize(800, 700)
        
        style_sheet = """
            QMessageBox { font-family: 'Consolas', 'Monaco', monospace; font-size: 13px; }
            QMessageBox QLabel { color: #FFFFFF; background-color: #2E2E2E; padding: 10px; border-radius: 5px; }
        """
        dialog.setStyleSheet(style_sheet)
        
        for widget in dialog.findChildren(QWidget):
            if isinstance(widget, QLabel):
                widget.setWordWrap(True)
                widget.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
                widget.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                break
        
        dialog.exec()

    def _format_full_summary_message(self) -> str:
        """Formats the text for the final, dynamic, multi-stage summary."""
        message_parts = ["🏆 FULL OPTIMIZATION SUMMARY 🏆\n"]
        total_removed = 0

        reporting_commented = self.reporting_summary_data['commented_count']
        reporting_total = self.reporting_summary_data['summary']['total_columns']
        reporting_pct = (reporting_commented / reporting_total * 100) if reporting_total > 0 else 0
        total_removed += reporting_commented
        
        stage1_msg = f"""==================================================
📊 STAGE 1: REPORTING LAYER CLEANUP
==================================================
• Columns removed from DBT Views: {reporting_commented:,} / {reporting_total:,}
• Reduction in this layer: {reporting_pct:.1f}%"""
        message_parts.append(stage1_msg)

        if self.marts_summary_data:
            marts_commented = self.marts_summary_data['commented_count']
            marts_failed = self.marts_summary_data['failed_count']

            marts_total_processed = self.marts_summary_data.get('total_processed', marts_commented + marts_failed)

            marts_pct = (marts_commented / marts_total_processed * 100) if marts_total_processed > 0 else 0
            total_removed += marts_commented

            stage2_msg = f"""
==================================================
🗂️ STAGE 2: MARTS LAYER OPTIMIZATION
==================================================
• Columns removed from DBT Models (related to Reporting): {marts_commented:,} / {marts_total_processed:,}
• Reduction in this layer: {marts_pct:.1f}%
• Failed to remove (due to dependencies): {marts_failed:,}"""
            message_parts.append(stage2_msg)

        if self.marts_only_summary_data:
            audit_commented = self.marts_only_summary_data['commented_count']
            audit_failed = self.marts_only_summary_data['failed_count']

            audit_total_processed = self.marts_only_summary_data.get('total_processed', audit_commented + audit_failed)

            audit_pct = (audit_commented / audit_total_processed * 100) if audit_total_processed > 0 else 0
            total_removed += audit_commented

            stage3_msg = f"""
==================================================
🔍 STAGE 3: MARTS INTERNAL AUDIT
==================================================
• Columns removed from DBT Models (internal audit): {audit_commented:,} / {audit_total_processed:,}
• Reduction in this layer: {audit_pct:.1f}%
• Failed to remove (due to dependencies): {audit_failed:,}"""
            message_parts.append(stage3_msg)

        final_summary_section = f"""
==================================================
✅ FINAL RESULT
==================================================
• Total columns removed across all completed stages: {total_removed:,}

🎉 Congratulations! You have successfully optimized the data model.
Check Git for all file changes.
"""
        message_parts.append(final_summary_section)
        
        return "\n".join(message_parts)

if __name__ == "__main__":
    QApplication.setOrganizationName("MyCompany")
    QApplication.setApplicationName("PowerBIAnalyzer")
    app = QApplication(sys.argv)
    
    app.setStyleSheet("""
        QWidget { background-color: #2E2E2E; color: #FFFFFF; font-size: 14px; }
        QMainWindow { background-color: #1E1E1E; }
        QLineEdit { background-color: #3C3C3C; border: 1px solid #555555; border-radius: 4px; padding: 5px; }
        QPushButton { background-color: #555555; border: 1px solid #666666; border-radius: 4px; padding: 5px 15px; }
        QPushButton:hover { background-color: #6A6A6A; }
        QPushButton#runAnalysisButton { background-color: #3A7BFF; font-weight: bold; padding: 8px; }
        QPushButton#runAnalysisButton:hover { background-color: #5A94FF; }
        QPushButton:disabled { background-color: #444444; color: #888888; }
        QHeaderView::section { background-color: #3C3C3C; padding: 4px; border: 1px solid #555555; }
        QTableWidget { gridline-color: #444444; border: 1px solid #555555; }
        QTableWidget::item { padding-left: 5px; }
        QTableWidget::item:selected { background-color: #5A94FF; }
        QProgressBar { border: 1px solid #555555; border-radius: 4px; text-align: center; }
        QProgressBar::chunk { background-color: #3A7BFF; border-radius: 3px; }
        QFrame[frameShape="5"] { color: #555555; }
        QCheckBox::indicator {
            width: 16px;
            height: 16px;
            border-radius: 4px;
            border: 1px solid #AAAAAA;
            background-color: #3C3C3C;
        }
        QCheckBox::indicator:hover {
            border: 1px solid #5A94FF;
        }
        QCheckBox::indicator:checked {
            background-color: #3A7BFF;
            border: 1px solid #3A7BFF;
        }
        QCheckBox::indicator:checked:hover {
            background-color: #5A94FF;
            border: 1px solid #5A94FF;
        }
        
        /* --- NOWE REGUŁY DLA PODŚWIETLENIA ZAKŁADEK --- */
        
        QTabWidget::pane {
            border-top: 2px solid #5A94FF;
            background-color: #3C3C3C;
        }

        QTabBar::tab {
            background: #444444;
            color: #BBBBBB;
            border: 1px solid #555555;
            border-bottom: none;
            border-top-left-radius: 4px;
            border-top-right-radius: 4px;
            padding: 8px 16px;
            margin-right: 2px;
        }

        QTabBar::tab:hover {
            background: #555555;
        }

        QTabBar::tab:selected {
            background: #3C3C3C;
            color: #FFFFFF;
            font-weight: bold;
            border: 1px solid #5A94FF;
            border-bottom: 1px solid #3C3C3C;
        }
    """)
    
    main_window = MainWindow()
    controller = AppController(view=main_window)
    main_window.show()
    sys.exit(app.exec())