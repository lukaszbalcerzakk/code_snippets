from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QHeaderView,
    QAbstractItemView, QCheckBox, QProgressBar, QTabWidget
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QIcon


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Power BI Usage Analyzer")
        
        self.setWindowIcon(QIcon("assets/icon2.png"))
        
        self.setMinimumWidth(900)
        
        # CORRECT: Complete flag combination for PyQt6 maximization
        self.setWindowFlags(
            Qt.WindowType.Window |
            Qt.WindowType.CustomizeWindowHint |
            Qt.WindowType.WindowTitleHint |
            Qt.WindowType.WindowSystemMenuHint |
            Qt.WindowType.WindowMinimizeButtonHint |
            Qt.WindowType.WindowMaximizeButtonHint |
            Qt.WindowType.WindowCloseButtonHint
        )

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)

        self._create_input_section()
        self._create_progress_bar_section()
        self._create_tabs_section()
        self._create_status_bar()

        self.progress_bar.hide()
        self.tabs.hide() 

    def _create_input_section(self):
        self.input_container = QWidget()
        container_layout = QVBoxLayout(self.input_container)
        container_layout.setContentsMargins(0,0,0,0)
        input_layout = QGridLayout()
        
        self.pbix_path_input = QLineEdit()
        self.pbix_path_input.setPlaceholderText("Path to your .zip or .pbix file...")
        self.pbix_path_input.setReadOnly(True)
        self.tabular_path_input = QLineEdit()
        self.tabular_path_input.setPlaceholderText("Path to your Tabular model folder...")
        self.tabular_path_input.setReadOnly(True)
        self.dbt_path_input = QLineEdit()
        self.dbt_path_input.setPlaceholderText("Path to your DBT models folder...")
        self.dbt_path_input.setReadOnly(True)
        self.pbix_browse_btn = QPushButton("Browse...")
        self.tabular_browse_btn = QPushButton("Browse...")
        self.dbt_browse_btn = QPushButton("Browse...")

        input_layout.addWidget(QLabel("Power BI Source File:"), 0, 0)
        input_layout.addWidget(self.pbix_path_input, 0, 1)
        input_layout.addWidget(self.pbix_browse_btn, 0, 2)
        input_layout.addWidget(QLabel("Tabular Model Path:"), 1, 0)
        input_layout.addWidget(self.tabular_path_input, 1, 1)
        input_layout.addWidget(self.tabular_browse_btn, 1, 2)
        input_layout.addWidget(QLabel("DBT Model Path:"), 2, 0)
        input_layout.addWidget(self.dbt_path_input, 2, 1)
        input_layout.addWidget(self.dbt_browse_btn, 2, 2)
        
        container_layout.addLayout(input_layout)
        self.run_analysis_btn = QPushButton("Run Analysis")
        self.run_analysis_btn.setObjectName("runAnalysisButton")
        self.run_analysis_btn.setEnabled(False)
        container_layout.addWidget(self.run_analysis_btn)
        
        self.main_layout.addWidget(self.input_container)
        self.main_layout.setStretchFactor(self.input_container, 0)

    def _create_progress_bar_section(self):
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("Analyzing... %p%")
        self.main_layout.addWidget(self.progress_bar)
        self.main_layout.setStretchFactor(self.progress_bar, 0)

    def _create_tabs_section(self):
        self.tabs = QTabWidget()
        
        # Tab 1: Reporting
        self.reporting_tab = QWidget()
        self.reporting_layout = QVBoxLayout(self.reporting_tab)
        self._create_reporting_results_section()
        self.tabs.addTab(self.reporting_tab, "📊 REPORTING")
        
        # Tab 2: Marts
        self.marts_tab = QWidget()
        self.marts_layout = QVBoxLayout(self.marts_tab)
        self._create_marts_results_section()
        self.tabs.addTab(self.marts_tab, "🗂️ MARTS")

        # Tab 3: Marts Audit
        self.marts_audit_tab = QWidget()
        self.marts_audit_layout = QVBoxLayout(self.marts_audit_tab) 
        self._create_marts_audit_section() 
        self.tabs.addTab(self.marts_audit_tab, "🔍 MARTS AUDIT")
        
        self.main_layout.addWidget(self.tabs)
        self.main_layout.setStretchFactor(self.tabs, 1)

    def _create_reporting_results_section(self):
        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("🔍 Filter:"))
        self.reporting_filter_input = QLineEdit()
        self.reporting_filter_input.setPlaceholderText("Type to filter by Table or Column name...")
        filter_layout.addWidget(self.reporting_filter_input)
        self.reporting_layout.addLayout(filter_layout)

        self.results_table = QTableWidget()
        self.results_table.setColumnCount(12)
        self.results_table.setHorizontalHeaderLabels([
            "Comment Out", "Table", "Column", "Is Used",
            "Visualization", "Measure", "Filter", 
            "Indirect Meas.", "Relationship", "Hierarchy",
            "Tabular Sort", "RLS"
        ])
        
        header = self.results_table.horizontalHeader()
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.results_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.results_table.setAlternatingRowColors(True)
        
        self.reporting_layout.addWidget(self.results_table)

        bottom_layout = QHBoxLayout()
        self.enable_live_mode_checkbox = QCheckBox("Enable Commenting Out (Live Mode)")
        self.enable_live_mode_checkbox.setEnabled(False)
        self.show_summary_btn = QPushButton("📊 Show Analysis Summary")
        self.show_summary_btn.setEnabled(False)
        self.apply_changes_btn = QPushButton("Apply Changes to REPORTING")
        self.apply_changes_btn.setEnabled(False)
        
        bottom_layout.addWidget(self.enable_live_mode_checkbox)
        bottom_layout.addWidget(self.show_summary_btn)
        bottom_layout.addWidget(self.apply_changes_btn)
        bottom_layout.addStretch()
        
        self.reporting_layout.addLayout(bottom_layout)

    def _create_marts_results_section(self):
        # Info label
        self.marts_info_label = QLabel(
            "Fields commented in REPORTING will appear here for MARTS analysis.\n"
            "Click 'Run Marts Analysis' to check dependencies."
        )
        self.marts_info_label.setStyleSheet("padding: 10px; background-color: #3C3C3C; border-radius: 5px;")
        self.marts_layout.addWidget(self.marts_info_label)

        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("🔍 Filter:"))
        self.marts_filter_input = QLineEdit()
        self.marts_filter_input.setPlaceholderText("Type to filter by Field name...")
        filter_layout.addWidget(self.marts_filter_input)
        self.marts_layout.addLayout(filter_layout)

        self.marts_table = QTableWidget()
        self.marts_table.setColumnCount(6)
        self.marts_table.setHorizontalHeaderLabels([
            "Comment Out", "Field", "Source Model", 
            "Used", "Blocked By", "Usage Example"
        ])

        header = self.marts_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        
        self.marts_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.marts_table.setAlternatingRowColors(True)
        
        self.marts_layout.addWidget(self.marts_table)

        self.marts_progress_bar = QProgressBar()
        self.marts_progress_bar.setRange(0, 100)
        self.marts_progress_bar.setValue(0)
        self.marts_progress_bar.setTextVisible(True)
        self.marts_progress_bar.setFormat("Analyzing MARTS... %p%")
        self.marts_progress_bar.hide()
        self.marts_layout.addWidget(self.marts_progress_bar)

        marts_buttons_layout = QHBoxLayout()
        
        self.run_marts_analysis_btn = QPushButton("🔍 Run Marts Analysis")
        self.run_marts_analysis_btn.setEnabled(False)
        self.run_marts_analysis_btn.setStyleSheet("""
            QPushButton { 
                background-color: #4A7BFF; 
                font-weight: bold; 
                padding: 8px;
            }
            QPushButton:hover { 
                background-color: #6A94FF; 
            }
            QPushButton:disabled { 
                background-color: #444444; 
                color: #888888;
            }
        """)
        
        self.apply_marts_changes_btn = QPushButton("💾 Apply Changes to MARTS")
        self.apply_marts_changes_btn.setEnabled(False)
        self.apply_marts_changes_btn.setStyleSheet("""
            QPushButton { 
                background-color: #FF7B3A; 
                font-weight: bold; 
                padding: 8px;
            }
            QPushButton:hover { 
                background-color: #FF945A; 
            }
            QPushButton:disabled { 
                background-color: #444444; 
                color: #888888;
            }
        """)
        
        marts_buttons_layout.addWidget(self.run_marts_analysis_btn)
        marts_buttons_layout.addWidget(self.apply_marts_changes_btn)
        marts_buttons_layout.addStretch()
        
        self.marts_layout.addLayout(marts_buttons_layout)

    def _create_marts_audit_section(self):
        """Creates the results section for the MARTS AUDIT tab"""
        
        # Info label
        self.marts_audit_info_label = QLabel(
            "This tab shows all fields from the MARTS layer that were not part of the initial analysis.\n"
            "Run analysis to check for internal dependencies before commenting out."
        )
        self.marts_audit_info_label.setStyleSheet("padding: 10px; background-color: #3C3C3C; border-radius: 5px;")
        self.marts_audit_layout.addWidget(self.marts_audit_info_label)

        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("🔍 Filter:"))
        self.marts_audit_filter_input = QLineEdit()
        self.marts_audit_filter_input.setPlaceholderText("Type to filter by Field name...")
        filter_layout.addWidget(self.marts_audit_filter_input)
        self.marts_audit_layout.addLayout(filter_layout)
        
        # Marts Audit table
        self.marts_audit_table = QTableWidget()
        self.marts_audit_table.setColumnCount(6)
        self.marts_audit_table.setHorizontalHeaderLabels([
            "Comment Out", "Field", "Source Model", 
            "Used", "Blocked By", "Usage Example"
        ])
        
        # Column configuration
        header = self.marts_audit_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        
        self.marts_audit_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.marts_audit_table.setAlternatingRowColors(True)
        
        self.marts_audit_layout.addWidget(self.marts_audit_table)

        # Progress bar for MARTS AUDIT analysis
        self.marts_audit_progress_bar = QProgressBar()
        self.marts_audit_progress_bar.setRange(0, 100)
        self.marts_audit_progress_bar.setValue(0)
        self.marts_audit_progress_bar.setTextVisible(True)
        self.marts_audit_progress_bar.setFormat("Analyzing MARTS... %p%")
        self.marts_audit_progress_bar.hide()
        self.marts_audit_layout.addWidget(self.marts_audit_progress_bar)
        
        # Buttons for marts audit
        marts_audit_buttons_layout = QHBoxLayout()
        
        self.run_marts_audit_analysis_btn = QPushButton("🔍 Run Marts Audit Analysis")
        self.run_marts_audit_analysis_btn.setEnabled(False)
        self.run_marts_audit_analysis_btn.setStyleSheet("""
            QPushButton { background-color: #4A7BFF; font-weight: bold; padding: 8px; }
            QPushButton:hover { background-color: #6A94FF; }
            QPushButton:disabled { background-color: #444444; color: #888888; }
        """)
        
        self.apply_marts_audit_changes_btn = QPushButton("💾 Apply Changes to MARTS")
        self.apply_marts_audit_changes_btn.setEnabled(False)
        self.apply_marts_audit_changes_btn.setStyleSheet("""
            QPushButton { background-color: #FF7B3A; font-weight: bold; padding: 8px; }
            QPushButton:hover { background-color: #FF945A; }
            QPushButton:disabled { background-color: #444444; color: #888888; }
        """)

        # The final summary button is now here
        self.show_full_summary_btn = QPushButton("🏆 Show Full Optimization Summary")
        self.show_full_summary_btn.hide() # Hidden by default
        
        marts_audit_buttons_layout.addWidget(self.run_marts_audit_analysis_btn)
        marts_audit_buttons_layout.addWidget(self.apply_marts_audit_changes_btn)
        marts_audit_buttons_layout.addStretch()
        marts_audit_buttons_layout.addWidget(self.show_full_summary_btn)
        
        self.marts_audit_layout.addLayout(marts_audit_buttons_layout)



    def _create_status_bar(self):
        self.statusBar().showMessage("Ready. Please select all required paths.")