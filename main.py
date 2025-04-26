# --- main_pull.py ---

import sys
import pandas as pd
import numpy as np
import yfinance as yf
import pyqtgraph as pg
from pyqtgraph.exporters import ImageExporter
from PyQt6 import QtWidgets, QtGui, QtCore
from PyQt6.QtCore import QThread, pyqtSignal
from datetime import datetime, time, timedelta, date  # Import date
import os
import pytz
import time as pytime
import traceback
import threading
import logging

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
)
# --- End Logging Setup ---

# --- Import Modules ---
try:
    from trend_analyzer import find_trend_lines, _has_scipy

    logging.info("Imported trend_analyzer.")
except ImportError as e:
    logging.error(f"Import trend_analyzer failed: {e}")
    _has_scipy = False
    find_trend_lines = lambda *a, **k: []
try:
    from playback_generator import (
        PlaybackGeneratorWorker,
        PLAYBACK_SPEEDS,
        DEFAULT_PLAYBACK_SPEED,
    )

    logging.info("Imported playback_generator.")
except ImportError as e:
    logging.error(f"Import playback_generator failed: {e}")
    PlaybackGeneratorWorker = None
    PLAYBACK_SPEEDS = {}
    DEFAULT_PLAYBACK_SPEED = ""
try:
    import tzlocal

    _has_tzlocal = True
    logging.info("tzlocal found.")
except ImportError:
    _has_tzlocal = False
    logging.info("tzlocal not found.")
# --- End Import Modules ---


# --- Custom Candlestick Item ---
class CandlestickItem(pg.GraphicsObject):
    """
    Custom GraphicsObject for displaying candlestick charts.
    Supports HOLLOW green / SOLID red style with matching colored wicks.
    """

    def __init__(self, data):
        pg.GraphicsObject.__init__(self)
        self.data = data
        self.picture = None
        self.generatePicture()

    def generatePicture(self):
        self.picture = QtGui.QPicture()
        if not self.data:
            p = QtGui.QPainter(self.picture)
            p.end()
            return
        p = QtGui.QPainter(self.picture)
        w = 0.6  # Default width
        if len(self.data) > 1:
            time_stamps = np.array([d["t"] for d in self.data])
            time_diffs = np.diff(time_stamps)
            valid_diffs = time_diffs[time_diffs > 0]
            if len(valid_diffs) > 0:
                median_diff = np.median(valid_diffs)
                if abs(median_diff - 86400) < 3600:
                    w = 86400 * 0.6  # Daily
                elif abs(median_diff - 604800) < 86400:
                    w = 604800 * 0.6  # Weekly
                else:
                    w = median_diff * 0.6  # Intraday
        pen_wick_up = pg.mkPen(color=(0, 200, 0), width=1)
        pen_wick_down = pg.mkPen(color=(200, 0, 0), width=1)
        pen_body_up = pg.mkPen(color=(0, 200, 0), width=1)
        brush_body_down = pg.mkBrush(200, 0, 0, 200)
        brush_hollow = pg.mkBrush(None)
        pen_solid_body_down = pg.mkPen(None)
        for d in self.data:
            t, o, h, l, c = d["t"], d["o"], d["h"], d["l"], d["c"]
            is_up = c > o
            wick_pen = pen_wick_up if is_up else pen_wick_down
            p.setPen(wick_pen)
            body_top = max(o, c)
            body_bottom = min(o, c)
            p.drawLine(QtCore.QPointF(t, h), QtCore.QPointF(t, body_top))
            p.drawLine(QtCore.QPointF(t, l), QtCore.QPointF(t, body_bottom))
            if is_up:
                p.setPen(pen_body_up)
                p.setBrush(brush_hollow)
            else:
                p.setPen(pen_solid_body_down)
                p.setBrush(brush_body_down)
            p.drawRect(QtCore.QRectF(t - w / 2, o, w, c - o))
        p.end()

    def paint(self, p, *args):
        if self.picture is None:
            self.generatePicture()
        if self.picture and not self.picture.isNull():
            p.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        if not self.data:
            return QtCore.QRectF()
        time_stamps = [d["t"] for d in self.data]
        min_t, max_t = min(time_stamps), max(time_stamps)
        width_factor = 0.6
        if len(self.data) > 1:
            time_diffs = np.diff(np.array(time_stamps))
            valid_diffs = time_diffs[time_diffs > 0]
            if len(valid_diffs) > 0:
                median_diff = np.median(valid_diffs)
                if abs(median_diff - 86400) < 3600:
                    width_factor = 86400 * 0.6
                elif abs(median_diff - 604800) < 86400:
                    width_factor = 604800 * 0.6
                else:
                    width_factor = median_diff * 0.6
        min_low = min(d["l"] for d in self.data)
        max_high = max(d["h"] for d in self.data)
        return QtCore.QRectF(
            min_t - width_factor / 2,
            min_low,
            (max_t - min_t) + width_factor,
            max_high - min_low,
        )

    def setData(self, data):
        self.data = data
        self.generatePicture()
        self.prepareGeometryChange()
        self.update()


# --- Main Application Window ---
class StockChartApp(QtWidgets.QMainWindow):
    ZOOM_LEVELS = {
        "4 Hours": 4 * 3600,
        "8 Hours": 8 * 3600,
        "12 Hours": 12 * 3600,
        "Full Day": 24 * 3600,
    }
    DEFAULT_ZOOM_LABEL = "4 Hours"
    FETCH_PERIODS = ["1mo", "3mo", "6mo", "1y", "2y", "5y", "ytd", "max"]
    CUSTOM_DATE_LABEL = "Custom Date"
    VALID_INTERVALS = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "1wk"]
    INTRADAY_INTERVALS = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"]
    DAILY_WEEKLY_INTERVALS = ["1d", "1wk"]
    DEFAULT_INTERVAL = "1d"
    DEFAULT_PERIOD = "1y"

    def __init__(self):
        super().__init__()
        logging.info("Initializing StockChartApp...")
        self.setWindowTitle("Stock Chart Viewer (Swing Focus)")
        self.setGeometry(100, 100, 1800, 800)
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QVBoxLayout(central_widget)
        controls_layout = QtWidgets.QHBoxLayout()
        main_layout.addLayout(controls_layout)

        # Input Controls
        input_group_box = QtWidgets.QGroupBox("Data Selection")
        input_layout = QtWidgets.QHBoxLayout(input_group_box)
        controls_layout.addWidget(input_group_box)
        self.ticker_label = QtWidgets.QLabel("Ticker:")
        self.ticker_input = QtWidgets.QLineEdit("AAPL")
        input_layout.addWidget(self.ticker_label)
        input_layout.addWidget(self.ticker_input)
        self.period_label = QtWidgets.QLabel("Period:")
        input_layout.addWidget(self.period_label)
        self.period_combo = QtWidgets.QComboBox()
        self.period_combo.addItem(self.CUSTOM_DATE_LABEL)
        self.period_combo.addItems(self.FETCH_PERIODS)
        self.period_combo.setCurrentText(self.DEFAULT_PERIOD)
        self.period_combo.currentIndexChanged.connect(self._update_ui_for_timeframe)
        input_layout.addWidget(self.period_combo)
        self.date_label = QtWidgets.QLabel("Date:")
        self.date_edit = QtWidgets.QDateEdit(calendarPopup=True)
        self.date_edit.setDisplayFormat("yyyy-MM-dd")
        self.date_edit.setDate(QtCore.QDate.currentDate().addDays(-1))
        input_layout.addWidget(self.date_label)
        input_layout.addWidget(self.date_edit)
        self.interval_label = QtWidgets.QLabel("Interval:")
        self.interval_input = QtWidgets.QComboBox()
        self.interval_input.addItems(self.VALID_INTERVALS)
        self.interval_input.setCurrentText(self.DEFAULT_INTERVAL)
        self.interval_input.currentIndexChanged.connect(self._update_ui_for_timeframe)
        input_layout.addWidget(self.interval_label)
        input_layout.addWidget(self.interval_input)
        self.fetch_button = QtWidgets.QPushButton("Fetch Plot")
        try:
            self.fetch_button.clicked.connect(self.fetch_and_plot_data)
            logging.info("Connected fetch_button signal.")
        except Exception as e:
            logging.error(f"FAILED to connect fetch_button signal: {e}", exc_info=True)
        input_layout.addWidget(self.fetch_button)

        # View Controls
        view_group_box = QtWidgets.QGroupBox("View Control")
        view_layout = QtWidgets.QHBoxLayout(view_group_box)
        controls_layout.addWidget(view_group_box)
        self.zoom_label = QtWidgets.QLabel("Zoom:")
        view_layout.addWidget(self.zoom_label)
        self.zoom_combo = QtWidgets.QComboBox()
        for label, duration_sec in self.ZOOM_LEVELS.items():
            self.zoom_combo.addItem(label, userData=duration_sec)
        self.zoom_combo.setCurrentText(self.DEFAULT_ZOOM_LABEL)
        self.zoom_combo.currentIndexChanged.connect(self._handle_zoom_change)
        view_layout.addWidget(self.zoom_combo)
        self.prev_chunk_button = QtWidgets.QPushButton("<< Prev")
        self.prev_chunk_button.setToolTip("Navigate backward (Intraday only)")
        self.prev_chunk_button.clicked.connect(self.prev_view)
        view_layout.addWidget(self.prev_chunk_button)
        self.current_view_label = QtWidgets.QLabel("View: (Fetch data)")
        self.current_view_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        view_layout.addWidget(self.current_view_label)
        self.next_chunk_button = QtWidgets.QPushButton("Next >>")
        self.next_chunk_button.setToolTip("Navigate forward (Intraday only)")
        self.next_chunk_button.clicked.connect(self.next_view)
        view_layout.addWidget(self.next_chunk_button)

        # Tools Controls
        tools_group_box = QtWidgets.QGroupBox("Tools")
        tools_layout = QtWidgets.QHBoxLayout(tools_group_box)
        controls_layout.addWidget(tools_group_box)
        self.trend_button = QtWidgets.QPushButton("Find Trends")
        self.trend_button.clicked.connect(self._trigger_find_and_draw_trends)
        self.trend_button.setEnabled(False)
        tools_layout.addWidget(self.trend_button)
        self.export_button = QtWidgets.QPushButton("Export View")
        self.export_button.clicked.connect(self.export_chart_to_png)
        self.export_button.setEnabled(False)
        tools_layout.addWidget(self.export_button)
        playback_vlayout = QtWidgets.QVBoxLayout()
        tools_layout.addLayout(playback_vlayout)
        playback_hlayout = QtWidgets.QHBoxLayout()
        playback_vlayout.addLayout(playback_hlayout)
        self.playback_speed_label = QtWidgets.QLabel("Speed:")
        playback_hlayout.addWidget(self.playback_speed_label)
        self.playback_speed_combo = QtWidgets.QComboBox()
        if PLAYBACK_SPEEDS:
            self.playback_speed_combo.addItems(PLAYBACK_SPEEDS.keys())
            self.playback_speed_combo.setCurrentText(DEFAULT_PLAYBACK_SPEED)
        else:
            self.playback_speed_combo.addItem("N/A")
            self.playback_speed_combo.setEnabled(False)
        playback_hlayout.addWidget(self.playback_speed_combo)
        playback_button_layout = QtWidgets.QHBoxLayout()
        playback_vlayout.addLayout(playback_button_layout)
        self.playback_generate_button = QtWidgets.QPushButton("Generate Playback")
        self.playback_generate_button.clicked.connect(self._start_playback_generation)
        self.playback_generate_button.setEnabled(False)
        playback_button_layout.addWidget(self.playback_generate_button)
        self.playback_cancel_button = QtWidgets.QPushButton("Cancel")
        self.playback_cancel_button.clicked.connect(self._cancel_playback_generation)
        self.playback_cancel_button.setEnabled(False)
        playback_button_layout.addWidget(self.playback_cancel_button)
        controls_layout.addStretch()

        # Plot Setup
        pg.setConfigOptions(antialias=True)
        self.plot_widget = pg.GraphicsLayoutWidget()
        main_layout.addWidget(self.plot_widget, stretch=1)
        self.price_plot = self.plot_widget.addPlot(row=0, col=0)
        self.price_plot.setLabel("left", "Price")
        self.price_plot.showGrid(x=True, y=True, alpha=0.2)
        self.price_plot.setDownsampling(mode="subsample")
        self.price_plot.setClipToView(True)
        self.price_plot.hideAxis("bottom")
        self.volume_plot = self.plot_widget.addPlot(row=1, col=0)
        self.volume_plot.setMaximumHeight(150)
        self.volume_plot.setLabel("left", "Volume")
        self.volume_plot.setLabel("bottom", "Time")
        self.volume_plot.showGrid(x=True, y=True, alpha=0.2)
        self.volume_plot.setDownsampling(mode="subsample")
        self.volume_plot.setClipToView(True)
        self.volume_plot.setXLink(self.price_plot)
        self.axis_item = pg.DateAxisItem(orientation="bottom")
        self.volume_plot.setAxisItems({"bottom": self.axis_item})
        price_vb = self.price_plot.getViewBox()
        price_vb.setMouseEnabled(x=True, y=True)
        volume_vb = self.volume_plot.getViewBox()
        volume_vb.setMouseEnabled(x=True, y=True)
        self.v_line = pg.InfiniteLine(
            angle=90,
            movable=False,
            pen=pg.mkPen("gray", style=QtCore.Qt.PenStyle.DashLine),
        )
        self.h_line = pg.InfiniteLine(
            angle=0,
            movable=False,
            pen=pg.mkPen("gray", style=QtCore.Qt.PenStyle.DashLine),
        )
        self.price_plot.addItem(self.v_line, ignoreBounds=True)
        self.price_plot.addItem(self.h_line, ignoreBounds=True)
        self.proxy = pg.SignalProxy(
            self.price_plot.scene().sigMouseMoved, rateLimit=60, slot=self._mouse_moved
        )

        # Status Bar
        status_layout = QtWidgets.QHBoxLayout()
        main_layout.addLayout(status_layout)
        self.statusBar = QtWidgets.QStatusBar()
        status_layout.addWidget(self.statusBar, stretch=1)
        self.progressBar = QtWidgets.QProgressBar()
        self.progressBar.setMaximum(100)
        self.progressBar.setValue(0)
        self.progressBar.setVisible(False)
        status_layout.addWidget(self.progressBar, stretch=0)
        self.statusBar.showMessage("Ready.")
        self.apply_dark_theme()

        # Internal State
        self.candlestick_item = None
        self.volume_item = None
        self._current_ticker = ""
        self._current_interval = ""
        self._current_period = ""
        self._fetched_data_tz = None
        self._full_data_start_ts = None
        self._full_data_end_ts = None
        self._current_view_start_ts = None
        self._current_view_end_ts = None
        try:
            self._current_zoom_duration_seconds = (
                self.zoom_combo.currentData()
                or self.ZOOM_LEVELS[self.DEFAULT_ZOOM_LABEL]
            )
        except Exception:
            self._current_zoom_duration_seconds = self.ZOOM_LEVELS[
                self.DEFAULT_ZOOM_LABEL
            ]
            logging.warning("Defaulting zoom duration.")
        self._current_stock_data = None
        self._local_tz = None
        self._trend_line_items = []
        self._playback_thread = None
        self._playback_worker = None
        self._playback_cancel_event = None
        self._playback_current_frame_exporter = None
        try:
            if _has_tzlocal:
                self._local_tz = tzlocal.get_localzone()
            else:
                self._local_tz = pytz.timezone(pytime.tzname[0])
            logging.info(
                f"Local timezone detected: {self._local_tz.zone}"
            )  # Use .zone attribute
        except AttributeError:  # Handle case where tzlocal returns object without .zone (e.g., Windows) or pytime fails
            try:  # Try getting name from tzlocal differently if possible
                tz_name = tzlocal.get_localzone_name()
                self._local_tz = pytz.timezone(tz_name)
                logging.info(f"Local timezone detected (name): {self._local_tz.zone}")
            except Exception as e_inner:
                fallback_tz = "America/New_York"
                logging.warning(
                    f"TZ detection failed (AttributeError/Inner): {e_inner}. Using '{fallback_tz}'."
                )
                try:
                    self._local_tz = pytz.timezone(fallback_tz)
                except Exception as ptz_e:
                    logging.error(f"Fallback TZ failed: {ptz_e}. Using UTC.")
                    self._local_tz = pytz.utc
        except Exception as e:  # Catch other potential errors during tz detection
            fallback_tz = "America/New_York"
            logging.warning(f"TZ detection failed (Outer): {e}. Using '{fallback_tz}'.")
            try:
                self._local_tz = pytz.timezone(fallback_tz)
            except Exception as ptz_e:
                logging.error(f"Fallback TZ failed: {ptz_e}. Using UTC.")
                self._local_tz = pytz.utc

        self._update_ui_for_timeframe()
        logging.info("StockChartApp initialization complete.")

    def apply_dark_theme(self):
        tos_bg_color_rgb = (27, 27, 27)
        self.price_plot.getViewBox().setBackgroundColor(tos_bg_color_rgb)
        self.volume_plot.getViewBox().setBackgroundColor(tos_bg_color_rgb)
        palette = QtGui.QPalette()
        palette.setColor(QtGui.QPalette.ColorRole.Window, QtGui.QColor(45, 45, 45))
        palette.setColor(
            QtGui.QPalette.ColorRole.WindowText, QtCore.Qt.GlobalColor.white
        )
        palette.setColor(QtGui.QPalette.ColorRole.Base, QtGui.QColor(30, 30, 30))
        palette.setColor(
            QtGui.QPalette.ColorRole.AlternateBase, QtGui.QColor(53, 53, 53)
        )
        palette.setColor(
            QtGui.QPalette.ColorRole.ToolTipBase, QtCore.Qt.GlobalColor.white
        )
        palette.setColor(
            QtGui.QPalette.ColorRole.ToolTipText, QtCore.Qt.GlobalColor.black
        )
        palette.setColor(QtGui.QPalette.ColorRole.Text, QtCore.Qt.GlobalColor.white)
        palette.setColor(QtGui.QPalette.ColorRole.Button, QtGui.QColor(53, 53, 53))
        palette.setColor(
            QtGui.QPalette.ColorRole.ButtonText, QtCore.Qt.GlobalColor.white
        )
        palette.setColor(QtGui.QPalette.ColorRole.BrightText, QtCore.Qt.GlobalColor.red)
        palette.setColor(QtGui.QPalette.ColorRole.Link, QtGui.QColor(42, 130, 218))
        palette.setColor(QtGui.QPalette.ColorRole.Highlight, QtGui.QColor(42, 130, 218))
        palette.setColor(
            QtGui.QPalette.ColorRole.HighlightedText, QtCore.Qt.GlobalColor.black
        )
        palette.setColor(
            QtGui.QPalette.ColorGroup.Disabled,
            QtGui.QPalette.ColorRole.Text,
            QtGui.QColor(127, 127, 127),
        )
        palette.setColor(
            QtGui.QPalette.ColorGroup.Disabled,
            QtGui.QPalette.ColorRole.ButtonText,
            QtGui.QColor(127, 127, 127),
        )
        palette.setColor(
            QtGui.QPalette.ColorGroup.Disabled,
            QtGui.QPalette.ColorRole.Base,
            QtGui.QColor(40, 40, 40),
        )
        palette.setColor(
            QtGui.QPalette.ColorGroup.Disabled,
            QtGui.QPalette.ColorRole.WindowText,
            QtGui.QColor(127, 127, 127),
        )
        app = QtWidgets.QApplication.instance()
        if app:
            app.setPalette(palette)
        style_sheet = """ QToolTip { color: black; background-color: lightyellow; border: 1px solid black; } QLineEdit, QDateEdit, QComboBox { padding: 5px; border: 1px solid #505050; border-radius: 3px; } QDateEdit::drop-down, QComboBox::drop-down { border: none; width: 20px; background-color: transparent; } QDateEdit::down-arrow, QComboBox::down-arrow { width: 12px; height: 12px; } QDateEdit QCalendarWidget QWidget { alternate-background-color: #454545; background-color: #353535; } QCalendarWidget QToolButton { color: white; background-color: #505050; } QCalendarWidget QMenu { background-color: #353535; color: white; } QCalendarWidget QSpinBox { color: white; background-color: #505050; } QCalendarWidget QAbstractItemView:enabled { color: white; } QCalendarWidget QAbstractItemView:disabled { color: #808080; } QPushButton { padding: 6px 15px; border: 1px solid #555; border-radius: 4px; background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 #606060, stop: 1 #404040); color: white; min-height: 20px; } QPushButton:hover { background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 #707070, stop: 1 #505050); border: 1px solid #666; } QPushButton:pressed { background-color: #353535; border: 1px solid #444; } QPushButton:disabled { background-color: #404040; border: 1px solid #444; color: #808080 } QStatusBar { color: lightgray; } QStatusBar QLabel { color: lightgray; } QProgressBar { border: 1px solid grey; border-radius: 3px; text-align: center; } QProgressBar::chunk { background-color: #4287f5; width: 1px; } QGroupBox { border: 1px solid gray; border-radius: 3px; margin-top: 0.5em; padding-top: 0.3em; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 3px 0 3px; } QScrollBar:vertical { border: 1px solid #444; background: #303030; width: 12px; margin: 0px; } QScrollBar::handle:vertical { background: #606060; min-height: 20px; border-radius: 6px; } QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0px; } QScrollBar:horizontal { border: 1px solid #444; background: #303030; height: 12px; margin: 0px; } QScrollBar::handle:horizontal { background: #606060; min-width: 20px; border-radius: 6px; } QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0px; } """
        if app:
            app.setStyleSheet(style_sheet)

    def fetch_and_plot_data(self):
        logging.debug("fetch_and_plot_data slot called.")
        ticker = self.ticker_input.text().upper().strip()
        period = self.period_combo.currentText()
        interval = self.interval_input.currentText()
        is_custom_date = period == self.CUSTOM_DATE_LABEL
        fetch_date_q = self.date_edit.date() if is_custom_date else None
        logging.info(
            f"Attempting fetch for Ticker: {ticker}, Period/Date: {'Custom: ' + fetch_date_q.toString('yyyy-MM-dd') if is_custom_date else period}, Interval: {interval}"
        )
        if not ticker:
            logging.warning("Fetch aborted: Ticker empty.")
            self.statusBar.showMessage("Error: Ticker empty.", 5000)
            QtWidgets.QMessageBox.warning(self, "Input Error", "Enter ticker.")
            return
        if interval not in self.VALID_INTERVALS:
            logging.warning(f"Fetch aborted: Invalid interval '{interval}'.")
            self.statusBar.showMessage(f"Error: Invalid interval '{interval}'.", 5000)
            QtWidgets.QMessageBox.warning(
                self, "Input Error", f"Invalid interval: {interval}."
            )
            return

        yf_kwargs = {"interval": interval}
        filter_data_by_date = False
        if is_custom_date:
            if fetch_date_q is None:
                logging.error("Custom Date selected but date edit value is None.")
                return
            start_str = fetch_date_q.addDays(-1).toString("yyyy-MM-dd")
            end_str = fetch_date_q.addDays(2).toString("yyyy-MM-dd")
            yf_kwargs["start"] = start_str
            yf_kwargs["end"] = end_str
            yf_kwargs["prepost"] = interval in self.INTRADAY_INTERVALS
            filter_data_by_date = True
            filter_start_dt = datetime.combine(fetch_date_q.toPyDate(), time.min)
            filter_end_dt = datetime.combine(fetch_date_q.toPyDate(), time.max)
            logging.debug(
                f"Using yfinance start/end: {start_str} to {end_str}, prepost={yf_kwargs['prepost']}"
            )
        else:
            yf_kwargs["period"] = period
            yf_kwargs["prepost"] = False
            logging.debug(f"Using yfinance period: {period}, prepost=False")

        self._current_ticker = ticker
        self._current_interval = interval
        self._current_period = period
        self.statusBar.showMessage(f"Fetching {ticker} ({interval} / {period})...", 0)
        QtWidgets.QApplication.processEvents()
        try:
            self.clear_plots()
            logging.debug(f"Calling yfinance Ticker().history() with args: {yf_kwargs}")
            tkr = yf.Ticker(ticker)
            stock_data_full = tkr.history(**yf_kwargs)
            logging.info(
                f"yfinance fetch complete. Raw data shape: {stock_data_full.shape}"
            )
            if stock_data_full.empty:
                logging.warning("yfinance returned empty DataFrame.")
                msg = f"No data from yfinance for {ticker} ({interval} / {period})."
                self.statusBar.showMessage(msg, 5000)
                QtWidgets.QMessageBox.warning(self, "Data Error", msg)
                return

            stock_data = stock_data_full
            data_tz = None
            if filter_data_by_date:
                logging.debug("Filtering fetched data to selected custom date...")
                is_datetime_index = isinstance(stock_data_full.index, pd.DatetimeIndex)
                filter_start_dt_aware = None
                filter_end_dt_aware = None
                if (
                    is_datetime_index
                    and hasattr(stock_data_full.index, "tz")
                    and stock_data_full.index.tz is not None
                ):
                    data_tz = stock_data_full.index.tz
                    logging.debug(f"Data TZ: {data_tz}")
                    try:
                        filter_start_dt_aware = data_tz.localize(filter_start_dt)
                        filter_end_dt_aware = data_tz.localize(filter_end_dt)
                    except Exception as dst_e:
                        logging.warning(f"DST issue: {dst_e}. Converting to UTC.")
                        stock_data_full.index = stock_data_full.index.tz_convert(
                            pytz.utc
                        )
                        data_tz = pytz.utc
                        filter_start_dt_aware = pytz.utc.localize(filter_start_dt)
                        filter_end_dt_aware = pytz.utc.localize(filter_end_dt)
                elif is_datetime_index:
                    logging.debug("Naive index. Localizing for filter...")
                    try:
                        stock_data_full.index = stock_data_full.index.tz_localize(
                            self._local_tz
                        )
                        data_tz = self._local_tz
                        filter_start_dt_aware = data_tz.localize(filter_start_dt)
                        filter_end_dt_aware = data_tz.localize(filter_end_dt)
                        logging.info(f"Localized to {data_tz.zone}")
                    except Exception:
                        logging.warning("Local localize failed. Trying UTC.")
                        try:
                            stock_data_full.index = stock_data_full.index.tz_localize(
                                pytz.utc
                            )
                            data_tz = pytz.utc
                            filter_start_dt_aware = pytz.utc.localize(filter_start_dt)
                            filter_end_dt_aware = pytz.utc.localize(filter_end_dt)
                            logging.info("Localized to UTC.")
                        except Exception:
                            logging.error("UTC localize failed.")
                            data_tz = None
                            filter_start_dt_aware = filter_start_dt
                            filter_end_dt_aware = filter_end_dt
                else:
                    logging.error("Index not DatetimeIndex.")
                    return
                logging.debug(
                    f"Filtering with range: {filter_start_dt_aware} to {filter_end_dt_aware}"
                )
                stock_data = stock_data_full[
                    (stock_data_full.index >= filter_start_dt_aware)
                    & (stock_data_full.index <= filter_end_dt_aware)
                ]
                logging.info(
                    f"Data shape after custom date filtering: {stock_data.shape}"
                )
                if stock_data.empty:
                    logging.warning("Data empty after custom date filtering.")
                    msg = f"No data found for {ticker} on {fetch_date_q.toString('yyyy-MM-dd')} after filtering."
                    self.statusBar.showMessage(msg, 5000)
                    QtWidgets.QMessageBox.warning(self, "Data Error", msg)
                    return
            else:
                if (
                    isinstance(stock_data.index, pd.DatetimeIndex)
                    and hasattr(stock_data.index, "tz")
                    and stock_data.index.tz is not None
                ):
                    data_tz = stock_data.index.tz
                    logging.debug(f"Data timezone for period fetch: {data_tz}")
                elif isinstance(stock_data.index, pd.DatetimeIndex):
                    logging.warning("Data index is naive for period fetch.")
                    data_tz = self._local_tz
                else:
                    logging.error("Index type is not DatetimeIndex for period fetch.")
                    data_tz = None

            logging.debug("Starting data cleaning...")
            initial_rows = len(stock_data)
            stock_data.dropna(
                subset=["Open", "High", "Low", "Close", "Volume"], inplace=True
            )
            rows_after_na = len(stock_data)
            logging.debug(
                f"Rows before/after dropna: {initial_rows}/{rows_after_na}. Dropped: {initial_rows - rows_after_na}"
            )
            if stock_data.empty:
                logging.warning("Data empty after dropna.")
                msg = f"Data for {ticker} ({interval}/{period}) contained only NaNs."
                self.statusBar.showMessage(msg, 5000)
                QtWidgets.QMessageBox.warning(self, "Data Error", msg)
                return
            stock_data.columns = [col.capitalize() for col in stock_data.columns]
            logging.debug(f"Columns capitalized: {stock_data.columns.tolist()}")
            required_cols = ["Open", "High", "Low", "Close", "Volume"]
            if not all(col in stock_data.columns for col in required_cols):
                missing = [c for c in required_cols if c not in stock_data.columns]
                logging.error(f"Missing required columns: {missing}")
                raise ValueError(f"Missing required columns: {missing}")

            self._current_stock_data = stock_data.copy()
            self._fetched_data_tz = data_tz
            logging.info(
                f"Stored processed stock data. Shape: {self._current_stock_data.shape}"
            )
            logging.debug("Converting final index to UTC timestamps...")
            time_stamps = None
            if isinstance(stock_data.index, pd.DatetimeIndex):
                try:
                    if (
                        hasattr(stock_data.index, "tz")
                        and stock_data.index.tz is not None
                    ):
                        time_stamps = (
                            stock_data.index.tz_convert(pytz.utc).astype(np.int64)
                            // 10**9
                        )
                    else:
                        time_stamps = (
                            stock_data.index.tz_localize(pytz.utc).astype(np.int64)
                            // 10**9
                        )
                except Exception:
                    logging.warning("TS localization failed, trying direct.")
                    try:
                        time_stamps = stock_data.index.astype(np.int64) // 10**9
                    except Exception as ts_err:
                        logging.error(f"TS conversion failed: {ts_err}", exc_info=True)
            else:
                logging.error("Final index not DatetimeIndex!")
            if time_stamps is None or len(time_stamps) == 0:
                logging.error("No valid timestamps generated.")
                self.statusBar.showMessage("Error processing timestamps.", 5000)
                return
            logging.debug(f"Timestamp conversion OK. Count: {len(time_stamps)}.")
            self._full_data_start_ts = time_stamps.min()
            self._full_data_end_ts = time_stamps.max()
            logging.info(
                f"Full data time range (UTC ts): {self._full_data_start_ts} to {self._full_data_end_ts}"
            )

            logging.debug("Preparing data for plot items...")
            bar_width = self._get_current_bar_width(interval)
            volume_data = np.nan_to_num(stock_data["Volume"].values)
            candlestick_data = [
                {
                    "t": time_stamps[i],
                    "o": stock_data["Open"].iloc[i],
                    "h": stock_data["High"].iloc[i],
                    "l": stock_data["Low"].iloc[i],
                    "c": stock_data["Close"].iloc[i],
                }
                for i in range(len(stock_data))
            ]
            logging.debug(
                f"Prepared {len(candlestick_data)} candle items. Bar width (vol): {bar_width:.2f}"
            )
            self.statusBar.showMessage(f"Plotting {ticker}...", 0)
            QtWidgets.QApplication.processEvents()

            logging.debug("Adding items to plots...")
            self.candlestick_item = CandlestickItem(candlestick_data)
            self.price_plot.addItem(self.candlestick_item)
            volume_brush = pg.mkBrush(0, 150, 200, 180)
            volume_pen = pg.mkPen(None)
            self.volume_item = pg.BarGraphItem(
                x=time_stamps,
                height=volume_data,
                width=bar_width,
                brush=volume_brush,
                pen=volume_pen,
            )
            self.volume_plot.addItem(self.volume_item)
            avg_interval_sec = (
                np.median(np.diff(time_stamps))
                if len(time_stamps) > 1
                else (86400 if interval in self.DAILY_WEEKLY_INTERVALS else 3600)
            )
            min_x_limit = self._full_data_start_ts - avg_interval_sec
            max_x_limit = self._full_data_end_ts + avg_interval_sec
            logging.debug(
                f"Setting plot X limits: {min_x_limit:.2f} to {max_x_limit:.2f}"
            )
            self.price_plot.setLimits(xMin=min_x_limit, xMax=max_x_limit)
            self.volume_plot.setLimits(xMin=min_x_limit, xMax=max_x_limit)

            if not is_custom_date or interval in self.DAILY_WEEKLY_INTERVALS:
                self._current_view_start_ts = self._full_data_start_ts
                self._current_view_end_ts = self._full_data_end_ts
                logging.debug("Setting initial view to full data range.")
            else:
                self._current_view_start_ts = self._full_data_start_ts
                self._current_view_end_ts = min(
                    self._full_data_start_ts + self._current_zoom_duration_seconds,
                    self._full_data_end_ts,
                )
                logging.debug(
                    f"Setting initial view based on zoom: {self._current_view_start_ts:.2f} - {self._current_view_end_ts:.2f}"
                )
            self._update_view()
            logging.debug(
                f"View X Range after initial _update_view: {self.price_plot.getViewBox().viewRange()[0]}"
            )

            logging.info(f"Plotting complete for {ticker}.")
            self.statusBar.showMessage(
                f"Plot updated for {ticker} ({interval}/{period}).", 5000
            )
            self.export_button.setEnabled(True)
            self.trend_button.setEnabled(True)
            self.playback_generate_button.setEnabled(
                PlaybackGeneratorWorker is not None
            )
        except Exception as e:
            logging.error(f"Error during fetch/plot: {e}", exc_info=True)
            self.statusBar.showMessage(f"Error: {type(e).__name__}", 8000)
            QtWidgets.QMessageBox.critical(
                self, f"Error", f"Fetch/Plot Error:\n{e}\n\nCheck logs."
            )
            self.clear_plots()

    def _update_ui_for_timeframe(self):
        selected_period = self.period_combo.currentText()
        selected_interval = self.interval_input.currentText()
        logging.debug(
            f"Updating UI for Period: '{selected_period}', Interval: '{selected_interval}'"
        )
        is_custom = selected_period == self.CUSTOM_DATE_LABEL
        is_daily_weekly = selected_interval in self.DAILY_WEEKLY_INTERVALS
        has_data = self._current_stock_data is not None
        self.date_label.setVisible(is_custom)
        self.date_edit.setVisible(is_custom)
        can_zoom_navigate = (not is_daily_weekly) and has_data
        self.zoom_label.setEnabled(can_zoom_navigate)
        self.zoom_combo.setEnabled(can_zoom_navigate)
        nav_visible = not is_daily_weekly
        self.prev_chunk_button.setVisible(nav_visible)
        self.next_chunk_button.setVisible(nav_visible)
        self.current_view_label.setVisible(nav_visible)
        if nav_visible and has_data:
            self.update_navigation_buttons()
        else:
            self.prev_chunk_button.setEnabled(False)
            self.next_chunk_button.setEnabled(
                False
            )  # Ensure disabled if hidden or no data
        logging.debug(
            f"UI Updated: DateEditVisible={is_custom}, ZoomEnabled={can_zoom_navigate}, NavVisible={nav_visible}"
        )

    def clear_plots(self):
        logging.debug("Clearing plots...")
        if self.candlestick_item:
            self.price_plot.removeItem(self.candlestick_item)
            self.candlestick_item = None
        self._clear_trend_lines_visuals()  # Call the specific method
        if self.volume_item:
            self.volume_plot.removeItem(self.volume_item)
            self.volume_item = None
        self.export_button.setEnabled(False)
        self.prev_chunk_button.setEnabled(False)
        self.next_chunk_button.setEnabled(False)
        self.trend_button.setEnabled(False)
        self.playback_generate_button.setEnabled(False)
        self.playback_cancel_button.setEnabled(False)
        self.current_view_label.setText("View: (Fetch data)")
        self.progressBar.setVisible(False)
        self._current_ticker = ""
        self._current_interval = ""
        self._current_period = ""
        self._fetched_data_tz = None
        self._full_data_start_ts = None
        self._full_data_end_ts = None
        self._current_view_start_ts = None
        self._current_view_end_ts = None
        self._current_stock_data = None
        self.price_plot.setLimits(xMin=None, xMax=None, yMin=None, yMax=None)
        self.volume_plot.setLimits(xMin=None, xMax=None, yMin=None, yMax=None)
        self._update_ui_for_timeframe()
        logging.debug("Plots cleared.")

    def export_chart_to_png(self, filename=None):
        if not self.candlestick_item:
            if filename is None:
                logging.warning("Manual export attempted with no chart data.")
                self.statusBar.showMessage("No chart data to export.", 3000)
            return None
        output_path = filename
        try:
            if output_path is None:
                logging.debug("Starting manual chart export...")
                export_dir = "Chart_Exports"
                os.makedirs(export_dir, exist_ok=True)
                ticker = self._current_ticker or "UNKNOWN"
                interval = self._current_interval or "NA"
                period_or_date = ""
                if self._current_period == self.CUSTOM_DATE_LABEL:
                    period_or_date = (
                        self.date_edit.date().toString("yyyyMMdd") or "UnknownDate"
                    )
                else:
                    period_or_date = self._current_period or "UnknownPeriod"

                start_fmt = (
                    "%Y%m%d" if interval in self.DAILY_WEEKLY_INTERVALS else "%H%M%S"
                )
                end_fmt = start_fmt
                start_time_str = self._format_timestamp(
                    self._current_view_start_ts, start_fmt, fallback="Start"
                )
                end_time_str = self._format_timestamp(
                    self._current_view_end_ts, end_fmt, fallback="End"
                )
                zoom_label = (
                    self.zoom_combo.currentText().replace(" ", "")
                    if self.zoom_combo.isEnabled()
                    else ""
                )
                safe_interval = (
                    interval.replace("m", "min")
                    .replace("h", "hr")
                    .replace("d", "day")
                    .replace("wk", "week")
                )
                suggested_filename = f"{ticker}_{safe_interval}_{period_or_date}_{start_time_str}-{end_time_str}{'_' + zoom_label if zoom_label else ''}.png"
                full_suggested_path = os.path.join(export_dir, suggested_filename)
                output_path, _ = QtWidgets.QFileDialog.getSaveFileName(
                    self,
                    "Save Chart As PNG",
                    full_suggested_path,
                    "PNG Files (*.png);;All Files (*)",
                )
                if not output_path:
                    logging.info("Manual export cancelled.")
                    self.statusBar.showMessage("Export cancelled.", 2000)
                    return None
            if not self.plot_widget or not self.plot_widget.scene():
                raise RuntimeError("Plot widget/scene not available.")
            logging.debug(f"Exporting chart scene to: {output_path}")
            exporter = self._playback_current_frame_exporter or ImageExporter(
                self.plot_widget.scene()
            )
            exporter.export(output_path)
            logging.info(f"Chart exported: {output_path}")
            if filename is None:
                self.statusBar.showMessage(
                    f"Chart exported: {os.path.basename(output_path)}", 5000
                )
            return output_path
        except Exception as e:
            log_prefix = "Playback Frame" if filename else "Manual Export"
            logging.error(f"Export failed ({log_prefix}): {e}", exc_info=True)
            if filename is None:
                self.statusBar.showMessage(f"Export Error: {e}", 8000)
                QtWidgets.QMessageBox.critical(
                    self, "Export Error", f"Failed export:\n{e}"
                )
            return None

    def next_view(self):
        if not self.next_chunk_button.isEnabled():
            return  # Respect UI state
        logging.debug("next_view called.")
        if self._current_view_end_ts is None or self._full_data_end_ts is None:
            return
        if self._current_view_end_ts >= self._full_data_end_ts:
            return
        duration = self._current_zoom_duration_seconds
        new_start = self._current_view_end_ts
        new_end = min(new_start + duration, self._full_data_end_ts)
        new_start = max(new_end - duration, self._full_data_start_ts)
        self._current_view_start_ts = new_start
        self._current_view_end_ts = new_end
        self._update_view()

    def prev_view(self):
        if not self.prev_chunk_button.isEnabled():
            return  # Respect UI state
        logging.debug("prev_view called.")
        if self._current_view_start_ts is None or self._full_data_start_ts is None:
            return
        if self._current_view_start_ts <= self._full_data_start_ts:
            return
        duration = self._current_zoom_duration_seconds
        new_end = self._current_view_start_ts
        new_start = max(new_end - duration, self._full_data_start_ts)
        new_end = min(new_start + duration, self._full_data_end_ts)
        self._current_view_start_ts = new_start
        self._current_view_end_ts = new_end
        self._update_view()

    def _handle_zoom_change(self):
        if not self.zoom_combo.isEnabled():
            return  # Respect UI state
        logging.debug("Zoom combo index changed.")
        if self._full_data_start_ts is None:
            return
        try:
            new_duration = self.zoom_combo.currentData()
            assert new_duration is not None
        except:
            logging.error("Invalid zoom data", exc_info=True)
            new_duration = self.ZOOM_LEVELS[self.DEFAULT_ZOOM_LABEL]
            self.zoom_combo.setCurrentText(self.DEFAULT_ZOOM_LABEL)
        self._current_zoom_duration_seconds = new_duration
        logging.info(
            f"Zoom level set to {self.zoom_combo.currentText()} ({new_duration}s)"
        )
        self._current_view_start_ts = self._full_data_start_ts
        self._current_view_end_ts = min(
            self._full_data_start_ts + self._current_zoom_duration_seconds,
            self._full_data_end_ts,
        )
        self._update_view()

    def _update_view(self):
        logging.debug("Updating plot view...")
        if (
            self._current_view_start_ts is not None
            and self._current_view_end_ts is not None
        ):
            self._update_x_range(self._current_view_start_ts, self._current_view_end_ts)
        else:
            logging.warning("_update_view called with invalid timestamps.")
            self.price_plot.autoRange()
        QtCore.QTimer.singleShot(0, self._update_y_range)
        QtCore.QTimer.singleShot(0, self._update_volume_y_range)
        self._update_ui_for_timeframe()
        self._update_view_label()

    def _update_x_range(self, start_ts, end_ts):
        if start_ts is None or end_ts is None:
            logging.warning("Update X range skip: None TS.")
            return
        try:
            interval = self._current_interval or self.DEFAULT_INTERVAL
            bar_width = self._get_current_bar_width(interval)
            view_padding = (
                bar_width * 0.5
                if interval in self.DAILY_WEEKLY_INTERVALS
                else bar_width * 2
            )  # Less padding for D/W
            if start_ts >= end_ts - view_padding * 2:
                view_padding = 0
            padded_start = start_ts - view_padding
            padded_end = end_ts + view_padding
            logging.debug(f"Setting X Range: {padded_start:.2f} to {padded_end:.2f}")
            self.price_plot.getViewBox().setXRange(padded_start, padded_end, padding=0)
        except Exception as e:
            logging.error(f"Error setting X range: {e}", exc_info=True)

    def update_navigation_buttons(self):
        if self.interval_input.currentText() in self.DAILY_WEEKLY_INTERVALS:
            self.prev_chunk_button.setEnabled(False)
            self.next_chunk_button.setEnabled(False)
            return
        tolerance = 1e-6
        can_go_prev = (
            self._current_view_start_ts is not None
            and self._full_data_start_ts is not None
            and self._current_view_start_ts > self._full_data_start_ts + tolerance
        )
        can_go_next = (
            self._current_view_end_ts is not None
            and self._full_data_end_ts is not None
            and self._current_view_end_ts < self._full_data_end_ts - tolerance
        )
        self.prev_chunk_button.setEnabled(can_go_prev)
        self.next_chunk_button.setEnabled(can_go_next)

    def _update_view_label(self):
        if not self.prev_chunk_button.isVisible():
            self.current_view_label.setText("")
            return
        start_str = self._format_timestamp(self._current_view_start_ts)
        end_str = self._format_timestamp(self._current_view_end_ts)
        self.current_view_label.setText(f"View: {start_str} - {end_str}")

    def _get_current_bar_width(self, interval_hint=""):
        if self._current_stock_data is None or len(self._current_stock_data) < 2:
            if interval_hint == "1d":
                return 86400 * 0.6
            elif interval_hint == "1wk":
                return 604800 * 0.6
            else:
                return 60 * 0.8
        if isinstance(self._current_stock_data.index, pd.DatetimeIndex):
            try:
                timestamps = self._current_stock_data.index.astype(np.int64) // 10**9
                diffs = np.diff(timestamps)
                valid_diffs = diffs[diffs > 0]
                median_diff = np.median(valid_diffs) if len(valid_diffs) > 0 else 0
                if abs(median_diff - 86400) < 3600:
                    width = 86400 * 0.6
                elif abs(median_diff - 604800) < 86400:
                    width = 604800 * 0.6
                elif median_diff > 0:
                    width = median_diff * 0.8
                else:
                    width = 60 * 0.8
                return width
            except Exception as e:
                logging.warning(f"Error calculating bar width: {e}")
                return 60 * 0.8
        else:
            logging.warning("Index not DatetimeIndex for bar width.")
            return 60 * 0.8

    def _format_timestamp(self, timestamp, fmt="%H:%M:%S", fallback="N/A"):
        if timestamp is None:
            return fallback
        try:
            dt_utc = datetime.fromtimestamp(timestamp, tz=pytz.utc)
            local_tz = self._local_tz or dt_utc.astimezone().tzinfo
            current_interval = self.interval_input.currentText()
            fmt = "%Y-%m-%d" if current_interval in self.DAILY_WEEKLY_INTERVALS else fmt
            return (
                dt_utc.astimezone(local_tz).strftime(fmt)
                if local_tz
                else dt_utc.strftime(fmt) + " (UTC)"
            )
        except Exception:
            try:
                return datetime.utcfromtimestamp(timestamp).strftime(fmt) + " (UTC?)"
            except:
                return fallback

    def _get_visible_data(self):
        if (
            self._current_stock_data is None
            or self._current_view_start_ts is None
            or self._current_view_end_ts is None
        ):
            return pd.DataFrame()
        try:
            start_utc = datetime.fromtimestamp(self._current_view_start_ts, tz=pytz.utc)
            end_utc = datetime.fromtimestamp(self._current_view_end_ts, tz=pytz.utc)
            data_tz = self._fetched_data_tz
            if data_tz:
                start_aware = start_utc.astimezone(data_tz)
                end_aware = end_utc.astimezone(data_tz)
                idx_comp = self._current_stock_data.index
            else:
                start_aware = start_utc.replace(tzinfo=None)
                end_aware = end_utc.replace(tzinfo=None)
                idx_comp = (
                    self._current_stock_data.index.tz_localize(None)
                    if isinstance(self._current_stock_data.index, pd.DatetimeIndex)
                    and self._current_stock_data.index.tz is not None
                    else self._current_stock_data.index
                )
            visible = self._current_stock_data[
                (idx_comp >= start_aware) & (idx_comp <= end_aware)
            ]
            return visible
        except Exception as e:
            logging.error(f"Error getting visible data: {e}", exc_info=True)
            return pd.DataFrame()

    def _update_y_range(self):
        visible_data = self._get_visible_data()
        if (
            visible_data.empty
            or "Low" not in visible_data
            or "High" not in visible_data
        ):
            self.price_plot.autoRange()
            return
        min_p = visible_data["Low"].min()
        max_p = visible_data["High"].max()
        if pd.isna(min_p) or pd.isna(max_p):
            self.price_plot.autoRange()
            return
        data_range = max_p - min_p
        padding = (
            data_range * 0.05
            if data_range > 1e-9
            else (abs(max_p) * 0.01 if abs(max_p) > 1e-9 else 0.1)
        )
        final_min = min_p - padding
        final_max = max_p + padding
        if pd.isna(final_min) or pd.isna(final_max) or final_min >= final_max:
            self.price_plot.autoRange()
        else:
            self.price_plot.getViewBox().setYRange(final_min, final_max, padding=0)

    def _update_volume_y_range(self):
        visible_data = self._get_visible_data()
        if visible_data.empty or "Volume" not in visible_data:
            self.volume_plot.autoRange()
            return
        max_v = pd.to_numeric(visible_data["Volume"], errors="coerce").max()
        if pd.isna(max_v):
            self.volume_plot.autoRange()
            return
        max_y = max_v * 1.1
        max_y = max(max_y, 10)
        if pd.isna(max_y) or max_y <= 0:
            self.volume_plot.autoRange()
        else:
            self.volume_plot.getViewBox().setYRange(0, max_y, padding=0)

    def _mouse_moved(self, evt):
        if not self.price_plot or not self.price_plot.sceneBoundingRect().contains(
            evt[0]
        ):
            self.v_line.hide()
            self.h_line.hide()
            return
        mouse_point = self.price_plot.getViewBox().mapSceneToView(evt[0])
        x_val, y_val = mouse_point.x(), mouse_point.y()
        self.v_line.setPos(x_val)
        self.h_line.setPos(y_val)
        self.v_line.show()
        self.h_line.show()
        current_interval = self.interval_input.currentText()
        fmt = (
            "%Y-%m-%d"
            if current_interval in self.DAILY_WEEKLY_INTERVALS
            else "%Y-%m-%d %H:%M:%S"
        )
        time_str = self._format_timestamp(x_val, fmt=fmt)
        if self._playback_thread is None:
            self.statusBar.showMessage(f"Time: {time_str}, Price: {y_val:.2f}")

    def _clear_trend_lines_visuals(self):
        logging.debug("Clearing trend line visuals.")
        for item in self._trend_line_items:
            try:
                self.price_plot.removeItem(item)
            except Exception:
                pass
        self._trend_line_items = []

    def _draw_trend_lines(self, trend_lines):
        logging.debug(f"Attempting to draw {len(trend_lines)} trend lines.")
        self._clear_trend_lines_visuals()
        drawn_count = 0
        if not trend_lines:
            return
        for line in trend_lines:
            if not all(
                k in line for k in ["type", "start_ts", "start_p", "end_ts", "end_p"]
            ):
                continue
            pen_color = (0, 255, 0, 180) if line["type"] == "up" else (255, 0, 0, 180)
            pen = pg.mkPen(
                color=pen_color, width=1.5, style=QtCore.Qt.PenStyle.DashLine
            )
            try:
                item = pg.PlotCurveItem(
                    x=[line["start_ts"], line["end_ts"]],
                    y=[line["start_p"], line["end_p"]],
                    pen=pen,
                )
                self.price_plot.addItem(item)
                self._trend_line_items.append(item)
                drawn_count += 1
            except Exception as e:
                logging.error(
                    f"Error creating/adding trend line item: {e}", exc_info=True
                )
        logging.info(f"Drew {drawn_count} trend lines.")
        msg = (
            f"Drew {drawn_count} trend lines."
            if drawn_count > 0
            else "Found 0 valid trend lines."
        )
        self.statusBar.showMessage(msg, 3000)

    def _trigger_find_and_draw_trends(self):
        logging.info("Triggering trend line analysis...")
        if not _has_scipy:
            logging.warning("Trend analysis aborted: Scipy missing.")
            self.statusBar.showMessage("Scipy missing for trends.", 5000)
            return
        if self._current_stock_data is None or self._current_stock_data.empty:
            logging.warning("Trend analysis aborted: No data.")
            self.statusBar.showMessage("No data for trends.", 3000)
            return
        self.statusBar.showMessage("Finding trend lines...", 0)
        QtWidgets.QApplication.processEvents()
        distance_param = 5
        prominence_param = None
        try:
            lines = find_trend_lines(
                self._current_stock_data.copy(),
                distance=distance_param,
                prominence=prominence_param,
            )
            logging.info(f"Trend analysis found {len(lines)} potential lines.")
        except Exception as e:
            logging.error(f"Trend analysis failed: {e}", exc_info=True)
            self.statusBar.showMessage(f"Trend Error: {e}", 8000)
            return
        self._draw_trend_lines(lines)

    def _start_playback_generation(self):
        logging.info("Starting playback generation process...")
        if PlaybackGeneratorWorker is None:
            logging.error("Playback module not loaded.")
            QtWidgets.QMessageBox.critical(
                self, "Error", "Playback module failed to load."
            )
            return
        if self._current_stock_data is None or self._current_stock_data.empty:
            logging.warning("Playback aborted: No data.")
            QtWidgets.QMessageBox.warning(self, "No Data", "Fetch chart data first.")
            return
        if self._playback_thread is not None:
            logging.warning("Playback aborted: Already running.")
            QtWidgets.QMessageBox.warning(
                self, "Busy", "Playback generation already in progress."
            )
            return
        default_filename = f"{self._current_ticker or 'chart'}_{self._current_interval or 'data'}_playback.gif"
        output_filename, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Save Playback As",
            default_filename,
            "GIF Files (*.gif);;MP4 Files (*.mp4);;All Files (*)",
        )
        if not output_filename:
            logging.info("Playback generation cancelled by user (file dialog).")
            self.statusBar.showMessage("Playback cancelled.", 2000)
            return
        logging.info(f"Playback output file selected: {output_filename}")
        speed = self.playback_speed_combo.currentText()
        interval_str = self.interval_input.currentText()
        try:
            if "m" in interval_str:
                interval_sec = int(interval_str.replace("m", "")) * 60
            elif "h" in interval_str:
                interval_sec = int(interval_str.replace("h", "")) * 3600
            elif "d" in interval_str:
                interval_sec = 86400
            elif "wk" in interval_str:
                interval_sec = 604800
            else:
                interval_sec = 60
        except:
            interval_sec = 60
        logging.debug(
            f"Playback parameters: Speed='{speed}', Interval(est)={interval_sec}s"
        )
        self.playback_cancel_event = threading.Event()
        self._playback_worker = PlaybackGeneratorWorker(
            stock_data_df=self._current_stock_data.copy(),
            output_filename=output_filename,
            speed_setting=speed,
            interval_seconds=interval_sec,
            cancel_event=self.playback_cancel_event,
        )
        self._playback_thread = QThread(
            self
        )  # Parent thread to self for better lifetime management
        self._playback_worker.moveToThread(self._playback_thread)
        logging.debug("Playback worker and thread created.")
        self._playback_worker.request_export_frame.connect(
            self._handle_export_frame_request
        )
        self._playback_worker.progress.connect(self._update_playback_progress)
        self._playback_worker.finished.connect(self._on_playback_finished)
        self._playback_thread.started.connect(self._playback_worker.run)
        self._playback_worker.finished.connect(
            self._playback_thread.quit
        )  # Worker signals thread to quit
        self._playback_worker.finished.connect(
            self._playback_worker.deleteLater
        )  # Schedule worker deletion
        self._playback_thread.finished.connect(
            self._playback_thread.deleteLater
        )  # Schedule thread deletion
        logging.debug("Playback signals connected.")
        self._set_controls_enabled(False)
        self.playback_cancel_button.setEnabled(True)
        self.progressBar.setValue(0)
        self.progressBar.setVisible(True)
        self.statusBar.showMessage("Starting playback generation...")
        self._playback_thread.start()
        logging.info("Playback thread started.")

    @QtCore.pyqtSlot(int, float, float, str)
    def _handle_export_frame_request(self, frame_num, start_ts, end_ts, frame_filepath):
        if self.playback_cancel_event and self.playback_cancel_event.is_set():
            logging.debug("Ignoring export request due to cancellation.")
            return
        try:
            self._update_x_range(start_ts, end_ts)
            self._update_y_range()
            self._update_volume_y_range()
            QtWidgets.QApplication.processEvents()
            if self._playback_current_frame_exporter is None:
                self._playback_current_frame_exporter = ImageExporter(
                    self.plot_widget.scene()
                )
            self.export_chart_to_png(filename=frame_filepath)
        except Exception as e:
            logging.error(
                f"ERROR handling export request for frame {frame_num}: {e}",
                exc_info=True,
            )
            if self._playback_worker:
                self.statusBar.showMessage(
                    f"Error generating frame {frame_num}. Cancelling.", 5000
                )
                self._cancel_playback_generation()

    @QtCore.pyqtSlot(int)
    def _update_playback_progress(self, percentage):
        self.progressBar.setValue(percentage)

    @QtCore.pyqtSlot(str)
    def _on_playback_finished(self, message):
        logging.info(f"Playback finished signal received: {message}")
        self.statusBar.showMessage(message, 10000)
        self.progressBar.setVisible(False)
        self._set_controls_enabled(True)
        self.playback_cancel_button.setEnabled(False)
        self._playback_thread = None
        self._playback_worker = None
        self._playback_cancel_event = None
        self._playback_current_frame_exporter = None
        if "Error" in message or "cancelled" in message:
            QtWidgets.QMessageBox.warning(self, "Playback Generation", message)
        else:
            QtWidgets.QMessageBox.information(self, "Playback Generation", message)

    def _cancel_playback_generation(self):
        logging.info("Attempting to cancel playback generation...")
        if self._playback_worker and self.playback_cancel_event:
            self.statusBar.showMessage("Cancelling playback generation...", 0)
            self.playback_cancel_event.set()
            self.playback_cancel_button.setEnabled(False)  # Disable immediately
        else:
            logging.warning("Cancel called but no playback worker/event found.")

    def _set_controls_enabled(self, enabled):
        logging.debug(f"Setting controls enabled state to: {enabled}")
        self.fetch_button.setEnabled(enabled)
        self.period_combo.setEnabled(enabled)
        self.interval_input.setEnabled(enabled)
        self.ticker_input.setEnabled(enabled)
        self.date_edit.setEnabled(enabled)  # Ensure period/interval/date are included
        is_daily_weekly = (
            self.interval_input.currentText() in self.DAILY_WEEKLY_INTERVALS
        )
        can_zoom_navigate = (
            enabled and (not is_daily_weekly) and (self._current_stock_data is not None)
        )
        self.zoom_combo.setEnabled(can_zoom_navigate)
        self.prev_chunk_button.setEnabled(
            can_zoom_navigate
            and self._current_view_start_ts > self._full_data_start_ts + 1e-6
        )
        self.next_chunk_button.setEnabled(
            can_zoom_navigate
            and self._current_view_end_ts < self._full_data_end_ts - 1e-6
        )
        self.trend_button.setEnabled(enabled and self._current_stock_data is not None)
        self.export_button.setEnabled(enabled and self._current_stock_data is not None)
        self.playback_generate_button.setEnabled(
            enabled
            and PlaybackGeneratorWorker is not None
            and self._current_stock_data is not None
        )
        self.playback_speed_combo.setEnabled(
            enabled and PlaybackGeneratorWorker is not None
        )

    def closeEvent(self, event):
        logging.info("Close event triggered. Cleaning up playback thread if active.")
        if self._playback_thread is not None and self._playback_thread.isRunning():
            logging.info("Signalling active playback thread to cancel and quit...")
            self._cancel_playback_generation()
            self._playback_thread.quit()
            if not self._playback_thread.wait(3000):
                logging.warning("Playback thread did not stop gracefully on close.")
            else:
                logging.info("Playback thread stopped.")
        else:
            logging.debug("No active playback thread to stop on close.")
        event.accept()


# --- Main Execution ---
if __name__ == "__main__":
    logging.info("Application starting...")
    try:
        import pytz
    except ImportError:
        logging.critical("CRITICAL: pytz not found.")
        sys.exit(1)
    if not _has_tzlocal:
        logging.warning("RECOMMENDATION: tzlocal not found.")
    if not _has_scipy:
        logging.warning("Scipy not found. Trends disabled.")
    if PlaybackGeneratorWorker is None:
        logging.warning("Playback feature disabled (import failed or imageio missing).")
    try:
        if hasattr(QtCore.Qt.ApplicationAttribute, "AA_EnableHighDpiScaling"):
            QtWidgets.QApplication.setAttribute(
                QtCore.Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True
            )
        if hasattr(QtCore.Qt.ApplicationAttribute, "AA_UseHighDpiPixmaps"):
            QtWidgets.QApplication.setAttribute(
                QtCore.Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True
            )
    except AttributeError:
        pass
    app = QtWidgets.QApplication(sys.argv)
    window = StockChartApp()
    window.show()
    logging.info("Entering main event loop...")
    exit_code = app.exec()
    logging.info(f"Application finished with exit code: {exit_code}")
    sys.exit(exit_code)
