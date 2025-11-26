import sys, os, time, traceback
from pathlib import Path
from functools import partial

from PyQt6 import QtCore, QtGui, QtWidgets, QtSvg

try:
    import pandas as pd
except:
    pd = None

try:
    import sqlparse
    from sqlparse import format as sql_format
except:
    sqlparse = None
    sql_format = None

# fuzzy ratio
try:
    import Levenshtein
    def fuzzy_ratio(a,b): return Levenshtein.ratio(a,b)
except:
    import difflib
    def fuzzy_ratio(a,b): return difflib.SequenceMatcher(None,a,b).ratio()

BASE = Path(__file__).resolve().parent
LOG_FILE = os.getenv("LOG_FILE", "oracle_to_mssql.log")
REPORT_EMAIL = "sanjusarkar@uiic.co.in"
APP_TITLE = "Oracle → MSSQL Data Migration"

# Autocomplete words & snippets
AUTOCOMP_WORDS = [
    "SELECT","FROM","WHERE","GROUP BY","ORDER BY","LIMIT","JOIN","LEFT JOIN",
    "RIGHT JOIN","INNER JOIN","ON","AS","DISTINCT","UNION","ALL","INSERT INTO",
    "VALUES","UPDATE","SET","DELETE","CREATE TABLE","ALTER TABLE","DROP TABLE","CASE",
    "WHEN","THEN","ELSE","END","WITH","HAVING"
]
SNIPPETS = {
    "sel": "SELECT *\nFROM ",
    "ins": "INSERT INTO  ()\nVALUES ();",
    "upd": "UPDATE \nSET \nWHERE ;",
    "jn": "JOIN  ON "
}



def append_colored(log_widget: QtWidgets.QTextEdit, text: str, color: str):
    cursor = log_widget.textCursor()
    cursor.movePosition(QtGui.QTextCursor.MoveOperation.End)
    fmt = QtGui.QTextCharFormat()
    fmt.setForeground(QtGui.QBrush(QtGui.QColor(color)))
    fmt.setFont(QtGui.QFont("Consolas", 10))
    cursor.insertText(text, fmt)
    log_widget.setTextCursor(cursor)
    log_widget.ensureCursorVisible()

# ---------------- ToggleSwitch (custom widget) ----------------
class ToggleSwitch(QtWidgets.QAbstractButton):
    """
    Custom drawn toggle switch that matches the screenshot style
    - flat pill background
    - white knob
    - theme-aware colors
    """
    def __init__(self, parent=None, checked=False):
        super().__init__(parent)
        self.setCheckable(True)
        self.setChecked(checked)
        self.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.setFixedSize(46, 26)  # pill size like screenshot

    def sizeHint(self):
        return QtCore.QSize(46, 26)

    def paintEvent(self, event):
        p = QtGui.QPainter(self)
        p.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing)
        r = self.rect()
        # Determine theme: try to find ancestor widget style
        is_dark = True
        w = self.window()
        if hasattr(w, "dark_theme"):
            is_dark = getattr(w, "dark_theme", True)
        # Colors tuned to screenshot
        if self.isChecked():
            bg = QtGui.QColor("#3fa659") if is_dark else QtGui.QColor("#3fa659")
        else:
            bg = QtGui.QColor("#2f3133") if is_dark else QtGui.QColor("#e8e8ea")
        # Draw background (rounded)
        radius = r.height() / 2
        rect_bg = QtCore.QRectF(r)
        p.setPen(QtCore.Qt.PenStyle.NoPen)
        p.setBrush(bg)
        p.drawRoundedRect(rect_bg, radius, radius)
        # Draw knob (white circle) at left or right
        knob_radius = r.height() - 8
        y = (r.height() - knob_radius) / 2
        if self.isChecked():
            x = r.width() - knob_radius - 4
        else:
            x = 4
        knob_rect = QtCore.QRectF(x, y, knob_radius, knob_radius)
        p.setBrush(QtGui.QColor("#ffffff"))
        # subtle border for dark theme
        if is_dark:
            p.setPen(QtGui.QColor(0,0,0,60))
        else:
            p.setPen(QtGui.QColor(0,0,0,30))
        p.drawEllipse(knob_rect)
        p.end()

# ---------------- SQL Highlighter ----------------
class SQLHighlighter(QtGui.QSyntaxHighlighter):
    def __init__(self, doc):
        super().__init__(doc)
        self.rules = []
        kwfmt = QtGui.QTextCharFormat()
        kwfmt.setForeground(QtGui.QColor("#9cdcfe"))
        kwfmt.setFontWeight(QtGui.QFont.Weight.Bold)
        keywords = [
            'select','from','where','and','or','order','by','group','having','limit',
            'join','inner','left','right','full','on','as','distinct','union','all',
            'case','when','then','else','end','insert','into','values','update','set',
            'delete','create','table','view','with','merge', 'alter'
        ]
        for kw in keywords:
            self.rules.append((QtCore.QRegularExpression(rf"(?i)\b{kw}\b"), kwfmt))
        numfmt = QtGui.QTextCharFormat(); numfmt.setForeground(QtGui.QColor("#b5cea8"))
        self.rules.append((QtCore.QRegularExpression(r"\b[0-9]+\b"), numfmt))
        strfmt = QtGui.QTextCharFormat(); strfmt.setForeground(QtGui.QColor("#ce9178"))
        self.rules.append((QtCore.QRegularExpression(r"'[^']*'"), strfmt))
        self.rules.append((QtCore.QRegularExpression(r'"[^"]*"'), strfmt))
        comfmt = QtGui.QTextCharFormat(); comfmt.setForeground(QtGui.QColor("#6a9955"))
        self.rules.append((QtCore.QRegularExpression(r"--[^\n]*"), comfmt))
        self.errfmt = QtGui.QTextCharFormat()
        try:
            self.errfmt.setUnderlineStyle(QtGui.QTextCharFormat.UnderlineStyle.WaveUnderline)
        except Exception:
            self.errfmt.setUnderlineStyle(QtGui.QTextCharFormat.UnderlineStyle.SingleUnderline)
        self.errfmt.setUnderlineColor(QtGui.QColor("#ff6b6b"))

    def highlightBlock(self, text: str):
        for pattern, fmt in self.rules:
            it = pattern.globalMatch(text)
            while it.hasNext():
                m = it.next()
                self.setFormat(m.capturedStart(), m.capturedLength(), fmt)

# ---------------- Editor + placeholder overlay + suggestions ----------------
class LineNumberArea(QtWidgets.QWidget):
    def __init__(self, editor):
        super().__init__(editor)
        self.editor = editor
    def sizeHint(self):
        return QtCore.QSize(self.editor.lineNumberAreaWidth(), 0)
    def paintEvent(self, event):
        self.editor.lineNumberAreaPaintEvent(event)

class SqlEditor(QtWidgets.QPlainTextEdit):
    placeholder = "-- Paste your Oracle SQL query here"

    def __init__(self):
        super().__init__()
        self.setFont(QtGui.QFont("Consolas", 11))
        self.setTabStopDistance(QtGui.QFontMetricsF(self.font()).horizontalAdvance(' ') * 4)
        self.lineNumberArea = LineNumberArea(self)
        self.blockCountChanged.connect(self.updateLineNumberAreaWidth)
        self.updateRequest.connect(self._on_update_request)
        self.cursorPositionChanged.connect(self.highlightCurrentLine)
        self.updateLineNumberAreaWidth(0)
        self.highlighter = SQLHighlighter(self.document())
        self.setWordWrapMode(QtGui.QTextOption.WrapMode.NoWrap)

        # suggestion popup
        self.sugg = QtWidgets.QListWidget()
        self.sugg.setWindowFlags(QtCore.Qt.WindowType.ToolTip)
        self.sugg.itemClicked.connect(self._complete_from_item)

        # ensure focus behavior
        self.setAcceptDrops(True)

    def sizeHint(self):
        return QtCore.QSize(600, 400)

    def lineNumberAreaWidth(self):
        digits = len(str(max(1, self.blockCount())))
        return 8 + self.fontMetrics().horizontalAdvance('9') * digits

    def updateLineNumberAreaWidth(self, _):
        self.setViewportMargins(self.lineNumberAreaWidth(), 0, 0, 0)

    def _on_update_request(self, rect, dy):
        if dy:
            self.lineNumberArea.scroll(0, dy)
        else:
            self.lineNumberArea.update(0, rect.y(), self.lineNumberArea.width(), rect.height())
        if rect.contains(self.viewport().rect()):
            self.updateLineNumberAreaWidth(0)

    def lineNumberAreaPaintEvent(self, event):
        painter = QtGui.QPainter(self.lineNumberArea)
        # pick bg according to theme
        window = self.window()
        is_dark = getattr(window, "dark_theme", True) if window else True
        bg = QtGui.QColor("#0f0f11") if is_dark else QtGui.QColor("#f5f6f7")
        painter.fillRect(event.rect(), bg)
        block = self.firstVisibleBlock()
        blockNumber = block.blockNumber()
        top = self.blockBoundingGeometry(block).translated(self.contentOffset()).top()
        bottom = top + self.blockBoundingRect(block).height()
        fm = self.fontMetrics()
        painter.setPen(QtGui.QColor("#6b6f75") if is_dark else QtGui.QColor("#8a8a8a"))
        while block.isValid() and top <= event.rect().bottom():
            if block.isVisible() and bottom >= event.rect().top():
                number = str(blockNumber + 1)
                painter.drawText(0, int(top), self.lineNumberArea.width()-6, fm.height(),
                                 QtCore.Qt.AlignmentFlag.AlignRight, number)
            block = block.next()
            top = bottom
            bottom = top + self.blockBoundingRect(block).height()
            blockNumber += 1

    def highlightCurrentLine(self):
        extras = []
        if not self.isReadOnly():
            sel = QtWidgets.QTextEdit.ExtraSelection()
            sel.format.setBackground(QtGui.QColor("#111214"))
            sel.cursor = self.textCursor()
            sel.cursor.clearSelection()
            extras.append(sel)
        self.setExtraSelections(extras)

    # placeholder overlay draw
    def paintEvent(self, event):
        super().paintEvent(event)
        if not self.toPlainText().strip():
            # draw placeholder text in editor's viewport
            painter = QtGui.QPainter(self.viewport())
            painter.setPen(QtGui.QColor("#6b6f75"))
            font = QtGui.QFont("Consolas", 11)
            font.setItalic(True)
            painter.setFont(font)
            margin = 6
            painter.drawText(margin+4, margin+18, self.placeholder)
            painter.end()

    def _current_word(self):
        tc = self.textCursor()
        tc.select(QtGui.QTextCursor.SelectionType.WordUnderCursor)
        return tc.selectedText()

    def _insert_snippet(self, snippet):
        tc = self.textCursor()
        tc.insertText(snippet)
        self.setTextCursor(tc)

    def show_suggestions(self, manual_prefix=None):
        prefix = manual_prefix if manual_prefix is not None else self._current_word()
        candidates = AUTOCOMP_WORDS + list(SNIPPETS.keys())
        scored = []
        pref = prefix.strip()
        for c in candidates:
            score = fuzzy_ratio(pref.lower(), c.lower()) if pref else 0.5
            if pref.lower() in c.lower(): score += 0.2
            if score > 0.2:
                scored.append((score, c))
        scored.sort(key=lambda x: -x[0])
        if not scored:
            self.sugg.hide()
            return
        self.sugg.clear()
        for s,c in scored[:12]:
            it = QtWidgets.QListWidgetItem(c)
            self.sugg.addItem(it)
        cr = self.cursorRect()
        pos = self.mapToGlobal(cr.bottomRight())
        self.sugg.move(pos)
        self.sugg.setFixedWidth(260)
        self.sugg.show()
        self.sugg.setFocus()

    def _complete_from_item(self, item):
        text = item.text()
        if text in SNIPPETS:
            self._insert_snippet(SNIPPETS[text])
        else:
            tc = self.textCursor()
            tc.select(QtGui.QTextCursor.SelectionType.WordUnderCursor)
            tc.removeSelectedText()
            tc.insertText(text + " ")
            self.setTextCursor(tc)
        self.sugg.hide()

    def keyPressEvent(self, e: QtGui.QKeyEvent):
        key = e.key()
        mods = e.modifiers()
        if key == QtCore.Qt.Key.Key_Tab:
            cur = self._current_word()
            if cur and cur.lower() in SNIPPETS:
                self._insert_snippet(SNIPPETS[cur.lower()]); return
            self.show_suggestions(); return
        if key == QtCore.Qt.Key.Key_Space and mods & QtCore.Qt.KeyboardModifier.ControlModifier:
            self.show_suggestions(); return
        if key in (QtCore.Qt.Key.Key_Return, QtCore.Qt.Key.Key_Enter):
            cursor = self.textCursor()
            block = cursor.block(); text = block.text()
            indent = len(text) - len(text.lstrip(' '))
            super().keyPressEvent(e)
            cursor = self.textCursor(); cursor.insertText(' ' * indent)
            return
        ch = e.text()
        if ch in ('(', '[', '{', '"', "'"):
            closing = {'(':')','[':']','{':'}','"':'"',"'" : "'"}[ch]
            super().keyPressEvent(e)
            self.insertPlainText(closing)
            cursor = self.textCursor(); cursor.movePosition(QtGui.QTextCursor.MoveOperation.Left)
            self.setTextCursor(cursor); return
        super().keyPressEvent(e)
        
        
    def resizeEvent(self, event):
        """Keep the lineNumberArea sized and positioned correctly."""
        super().resizeEvent(event)
        cr = self.contentsRect()
        # position the lineNumberArea at the left of the editor's content rect
        self.lineNumberArea.setGeometry(
            QtCore.QRect(cr.left(), cr.top(), self.lineNumberAreaWidth(), cr.height())
        )
        
    def wheelEvent(self, event):
        """Allow normal wheel scrolling and then sync minimap (if present)."""
        # let QPlainTextEdit handle the wheel (scroll)
        super().wheelEvent(event)
        # if a minimap exists on the same parent window, update its scrollbar
        try:
            mm = getattr(self.window(), "minimap", None)
            if mm is not None:
                # trigger minimap to recalc and sync its scrollbar position
                mm._sync_from_editor(0)
        except Exception:
            pass



# ---------------- Minimap ----------------
class MiniMap(QtWidgets.QPlainTextEdit):
    def __init__(self, editor: SqlEditor):
        super().__init__()
        self.editor = editor
        self.setReadOnly(True)
        self.setFont(QtGui.QFont("Consolas", 6))
        self.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setStyleSheet("background:transparent; color:#7d7d7d;")
        # sync text and scroll
        self._ignore_editor_scroll = False
        self.editor.textChanged.connect(self._sync_text)
        self.editor.verticalScrollBar().valueChanged.connect(self._on_editor_scrolled)
        self.verticalScrollBar().valueChanged.connect(self._on_minimap_scrolled)
        self.setPlainText(self.editor.toPlainText())

    def _sync_text(self):
        ed_sb = self.editor.verticalScrollBar()
        cur_val = ed_sb.value()
        self.setPlainText(self.editor.toPlainText())
        if ed_sb.maximum():
            ratio = cur_val / (ed_sb.maximum() or 1)
            mm_sb = self.verticalScrollBar()
            mm_sb.setValue(int(ratio * (mm_sb.maximum() or 1)))

    def _sync_from_editor(self, _val):
        ed = self.editor.verticalScrollBar()
        mm = self.verticalScrollBar()
        if ed.maximum() == 0:
            mm.setValue(0); return
        ratio = ed.value() / (ed.maximum() or 1)
        mm.setValue(int(ratio * (mm.maximum() or 1)))

    def _sync_to_editor(self, val):
        mm = self.verticalScrollBar()
        ed = self.editor.verticalScrollBar()
        if mm.maximum() == 0: return
        ratio = val / (mm.maximum() or 1)
        ed.setValue(int(ratio * (ed.maximum() or 1)))

    def mousePressEvent(self, ev):
        y = ev.position().y()
        h = self.viewport().height()
        ratio = max(0.0, min(1.0, y / h))
        ed = self.editor.verticalScrollBar()
        ed.setValue(int(ratio * (ed.maximum() or 1)))
        
        
    def _on_editor_scrolled(self, _val):
        # editor scrolled -> update minimap proportionally, but avoid looping
        if self._ignore_editor_scroll:
            return
        ed = self.editor.verticalScrollBar()
        mm = self.verticalScrollBar()
        if ed.maximum() == 0:
            mm.setValue(0); return
        ratio = ed.value() / (ed.maximum() or 1)
        mm.setValue(int(ratio * (mm.maximum() or 1)))

    def _on_minimap_scrolled(self, val):
        # minimap scrolled by user -> set editor position, but avoid loop
        try:
            self._ignore_editor_scroll = True
            mm = self.verticalScrollBar()
            ed = self.editor.verticalScrollBar()
            if mm.maximum() == 0:
                return
            ratio = val / (mm.maximum() or 1)
            ed.setValue(int(ratio * (ed.maximum() or 1)))
        finally:
            # delay clearing flag slightly to avoid race
            QtCore.QTimer.singleShot(30, lambda: setattr(self, "_ignore_editor_scroll", False))


# ---------------- Log tail worker ----------------
class LogTailWorker(QtCore.QThread):
    new_line = QtCore.pyqtSignal(str)
    def __init__(self, path):
        super().__init__()
        self.path = path
        self._stop = False
    def run(self):
        Path(self.path).touch(exist_ok=True)
        with open(self.path, "r", encoding="utf-8", errors="ignore") as f:
            f.seek(0, os.SEEK_END)
            while not self._stop:
                line = f.readline()
                if line:
                    self.new_line.emit(line)
                else:
                    time.sleep(0.5)
    def stop(self):
        self._stop = True

# ---------------- Migration worker ----------------
class MigrationWorker(QtCore.QThread):
    progress = QtCore.pyqtSignal(str)
    done = QtCore.pyqtSignal(object)
    failed = QtCore.pyqtSignal(str)
    def __init__(self, dbo, table, query, save_csv):
        super().__init__()
        self.dbo = dbo; self.table = table; self.query = query; self.save_csv = save_csv
    def run(self):
        try:
            try:
                self.progress.emit("[DEBUG] Query will be passed in-memory.\n")
            except Exception as e:
                self.progress.emit(f"[WARN] Could not write query.py: {e}\n")
            self.progress.emit("[INFO] Starting migration...\n")
            try:
                from main import main as user_main
            except Exception:
                def user_main(dbo, table, save_csv=False):
                    import pandas as _pd; time.sleep(0.8)
                    df = _pd.DataFrame({"id":[1,2,3],"val":["a","b","c"]})
                    if save_csv:
                        df.to_csv("migration_sample.csv", index=False, encoding="utf-8-sig")
                    return df
            df = user_main(self.dbo, self.table, self.query, save_csv=self.save_csv)
            self.progress.emit("[SUCCESS] Migration finished successfully.\n")
            self.done.emit(df)
        except Exception as e:
            tb = traceback.format_exc()
            self.progress.emit(f"[ERROR] Exception during migration:\\n{tb}\n")
            self.failed.emit(str(e))

# ---------------- Main Window ----------------
class MigrationWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowIcon(QtGui.QIcon("logo.ico"))
        self.setWindowTitle(APP_TITLE)
        self.resize(1280, 820)
        self.dark_theme = True

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        outer = QtWidgets.QVBoxLayout(central); outer.setContentsMargins(8,8,8,8)
        
        self.theme_btn = QtWidgets.QPushButton()
        self.theme_btn.setFixedSize(36, 28)
        self.theme_btn.setToolTip("Toggle theme (Dark / Light)")
        self.theme_btn.clicked.connect(self.toggle_theme)

        ico_theme = self.load_theme_icon("theme", 16)
        if not ico_theme.isNull():
            self.theme_btn.setIcon(ico_theme)


        # header
        header_container = QtWidgets.QVBoxLayout()
        title = QtWidgets.QLabel(APP_TITLE)
        title.setFont(QtGui.QFont("Segoe UI Semibold", 15))

        subtitle = QtWidgets.QLabel("© United India Insurance Company Limited")
        subtitle.setFont(QtGui.QFont("Segoe UI", 10))
        subtitle.setObjectName("subtitle")
        subtitle.setStyleSheet("color: #7d7d7d;")  # adapt to light/dark in theme function later

        # Row containing only theme button (aligned right)
        top_row = QtWidgets.QHBoxLayout()
        top_row.addWidget(title)
        top_row.addStretch()
        top_row.addWidget(self.theme_btn)

        header_container.addLayout(top_row)
        header_container.addWidget(subtitle)

        outer.addLayout(header_container)


        # main content
        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        outer.addWidget(splitter, 1)

        # left sidebar
        side = QtWidgets.QWidget()
        sl = QtWidgets.QVBoxLayout(side); sl.setContentsMargins(12,12,12,12)
        lbl_db = QtWidgets.QLabel("Target Database")
        lbl_db.setFont(QtGui.QFont("Segoe UI", 10, QtGui.QFont.Weight.Bold))
        db_name = QtWidgets.QLabel(os.getenv("SQL_DATABASE", "Actuarial"))
        db_name.setStyleSheet("color:#3f82e0;")
        sl.addWidget(lbl_db); sl.addWidget(db_name)
        form = QtWidgets.QFormLayout()
        self.schema = QtWidgets.QLineEdit(); self.schema.setPlaceholderText("Schema (e.g. dbo)")
        self.table = QtWidgets.QLineEdit(); self.table.setPlaceholderText("Table name")
        form.addRow("Schema Name:", self.schema); form.addRow("Table Name:", self.table)
        sl.addLayout(form)

        # Auto-save toggle with label (matches screenshot)
        toggle_row = QtWidgets.QHBoxLayout()
        self.auto_toggle = ToggleSwitch(checked=False)
        lbl_toggle = QtWidgets.QLabel("Auto save fetched data as CSV at runtime")
        lbl_toggle.setWordWrap(True)
        lbl_toggle.setFixedWidth(200)
        toggle_row.addWidget(self.auto_toggle)
        toggle_row.addSpacing(8)
        toggle_row.addWidget(lbl_toggle)
        toggle_row.addStretch()
        sl.addLayout(toggle_row)

        sl.addStretch()

        # action buttons (concise)
        self.start_btn = QtWidgets.QPushButton("Start Migration")
        ico = self.load_theme_icon("play", 16); 
        if not ico.isNull(): self.start_btn.setIcon(ico)
        self.start_btn.setFixedHeight(44); self.start_btn.clicked.connect(self.start_migration)
        sl.addWidget(self.start_btn)

        self.export_btn = QtWidgets.QPushButton("Export to CSV")
        exico = self.load_theme_icon("export", 16)
        if not exico.isNull(): self.export_btn.setIcon(exico)
        self.export_btn.setFixedHeight(40); self.export_btn.setEnabled(False)
        self.export_btn.clicked.connect(self.export_csv)
        sl.addWidget(self.export_btn)

        self.report_btn = QtWidgets.QPushButton("Report Issue")
        ri = self.load_theme_icon("report", 16)
        if not ri.isNull(): self.report_btn.setIcon(ri)
        self.report_btn.setFixedHeight(40); self.report_btn.clicked.connect(self.report_issue)
        sl.addWidget(self.report_btn)
        
        # Keyboard Shortcuts button
        self.shortcuts_btn = QtWidgets.QPushButton("Keyboard Shortcuts")
        scico = self.load_theme_icon("info", 16)  # OPTIONAL: provide icons/info.svg
        if not scico.isNull(): self.shortcuts_btn.setIcon(scico)
        self.shortcuts_btn.setFixedHeight(40)
        self.shortcuts_btn.clicked.connect(self.show_shortcuts_dialog)
        sl.addWidget(self.shortcuts_btn)


        footer = QtWidgets.QLabel("Developed and Maintained by Actuarial Department, HO\nVersion 2.0.1 © UIIC")
        footer.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)
        footer.setFont(QtGui.QFont("Segoe UI", 8))
        footer.setStyleSheet("color:#6d6d6e;")
        footer.setWordWrap(True)
        sl.addWidget(footer)


        splitter.addWidget(side)

        # right area (editor + minimap + logs)
        right = QtWidgets.QWidget(); rl = QtWidgets.QVBoxLayout(right); rl.setContentsMargins(8,8,8,8)
        # editor header
        eh = QtWidgets.QHBoxLayout()
        eh.addWidget(QtWidgets.QLabel("SQL Query Editor"))
        eh.addStretch()
        btn_format = QtWidgets.QPushButton("Format Query")
        btn_format.setFixedHeight(28); btn_format.clicked.connect(self.format_sql)
        eh.addWidget(btn_format)
        rl.addLayout(eh)

        # editor area
        editor_h = QtWidgets.QHBoxLayout()
        self.editor = SqlEditor()
        editor_frame = QtWidgets.QFrame(); ef_l = QtWidgets.QVBoxLayout(editor_frame); ef_l.setContentsMargins(6,6,6,6)
        ef_l.addWidget(self.editor)
        editor_h.addWidget(editor_frame, 5)

        self.minimap = MiniMap(self.editor)
        self.minimap.setFixedWidth(140)
        editor_h.addWidget(self.minimap, 1)

        rl.addLayout(editor_h, 8)

        # logs header + clear
        lh = QtWidgets.QHBoxLayout(); lh.addWidget(QtWidgets.QLabel("Migration Logs")); lh.addStretch()
        clear_btn = QtWidgets.QPushButton("Clear Logs"); clear_btn.setFixedHeight(26)
        clear_btn.clicked.connect(lambda: self.log.clear()); lh.addWidget(clear_btn)
        rl.addLayout(lh)

        self.log = QtWidgets.QTextEdit(); self.log.setReadOnly(True); self.log.setLineWrapMode(QtWidgets.QTextEdit.LineWrapMode.NoWrap);self.log.setPlainText("Waiting for migration to start..."); self.log.setObjectName("log")
        rl.addWidget(self.log, 3)

        splitter.addWidget(right)
        splitter.setStretchFactor(0,0); splitter.setStretchFactor(1,1)

        # state
        self.migration_worker = None; self.last_df = None

        # log tailer
        self.logtail = LogTailWorker(LOG_FILE)
        self.logtail.new_line.connect(self.on_new_log_line)
        self.logtail.start()

        # shortcuts
        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+Return"), self, activated=self.start_migration)
        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+S"), self, activated=self.export_csv)
        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+Space"), self, activated=self.editor.show_suggestions)
        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+Shift+F"), self, activated=self.format_sql)

        # initial: empty editor (placeholder draws overlay)
        self.editor.setPlainText("")

        # apply theme dark by default
        self.apply_theme(dark=True)

    # ---------------- theme ----------------
    def apply_theme(self, dark=True):
        self.dark_theme = dark
        if dark:
            self.setStyleSheet("""
                QWidget{background:#0f0f11;color:#e5e5e8;font-family:Consolas,Segoe UI;}
                QLineEdit,QPlainTextEdit{background:#0f0f11;color:#e5e5e8;border:1px solid #202024;padding:6px;border-radius:6px;}
                QPushButton{
                    background:#0f1113;
                    color:#e5e5e8;
                    border:1px solid #26262a;
                    padding:6px;
                    border-radius:6px;
                }
                QPushButton:hover{
                    background:#1a1b1e;
                    border:1px solid #3a3a3f;
                }
                QPushButton:pressed{
                    background:#202124;
                    border:1px solid #505057;
                }
                QLabel#subtitle {
                    color: #7d7d7d;
                }

                QTextEdit#log{background:#050607;color:#a8ffb2;border:1px solid #151518;padding:6px;}
            """)
        else:
            # Minimal Light Mode (C)
            self.setStyleSheet("""
                QWidget{background:#fafbfb;color:#0b0b0b;font-family:Consolas,Segoe UI;}
                QLineEdit,QPlainTextEdit{background:#ffffff;color:#0b0b0b;border:1px solid #e6e7ea;padding:6px;border-radius:6px;}
                QPushButton{
                    background:#ffffff;
                    color:#0b0b0b;
                    border:1px solid #e6e7ea;
                    padding:6px;
                    border-radius:6px;
                }
                QPushButton:hover{
                    background:#f4f5f6;
                    border:1px solid #d0d0d2;
                }
                QPushButton:pressed{
                    background:#e8e9ea;
                    border:1px solid #b9b9bb;
                }
                QLabel#subtitle {
                    color: #8c8c8f;
                }

                QTextEdit#log{background:#f3f7f6;color:#01321a;border:1px solid #e6e7ea;padding:6px;}
            """)
        # theme icon optional
        ico = self.load_theme_icon("theme", 16)
        if not ico.isNull(): self.theme_btn.setIcon(ico)
        self.start_btn.setIcon(self.load_theme_icon("play"))
        self.export_btn.setIcon(self.load_theme_icon("export"))
        self.report_btn.setIcon(self.load_theme_icon("report"))
        self.theme_btn.setIcon(self.load_theme_icon("theme"))
        self.shortcuts_btn.setIcon(self.load_theme_icon("info"))


    def toggle_theme(self):
        self.apply_theme(not getattr(self, "dark_theme", True))

    # ---------------- logs ----------------
    def on_new_log_line(self, line):
        ll = line.lower()
        if "error" in ll or "[error]" in ll or "traceback" in ll:
            append_colored(self.log, line, "#ff6b6b")
        elif "success" in ll or "completed" in ll or "[success]" in ll or "ok" in ll:
            append_colored(self.log, line, "#67e667")
        elif "warn" in ll or "warning" in ll:
            append_colored(self.log, line, "#f7d06b")
        elif "info" in ll:
            append_colored(self.log, line, "#7fb6ff")
        else:
            append_colored(self.log, line, "#c7c7c7")

    # ---------------- actions ----------------
    def start_migration(self):
        dbo = self.schema.text().strip()
        tbl = self.table.text().strip()
        q = self.editor.toPlainText().strip()
        if not dbo:
            QtWidgets.QMessageBox.warning(self, "Missing Schema", "Please enter schema name.")
            return
        if not tbl:
            QtWidgets.QMessageBox.warning(self, "Missing Table Name", "Please enter table name.")
            return
        if not q:
            QtWidgets.QMessageBox.warning(self, "Missing Query", "SQL query cannot be empty.")
            return
        if not self.validate_query(q):
            return
        self.start_btn.setEnabled(False)
        self.start_btn.setText("Migrating...")
        self.export_btn.setEnabled(False)
        self.auto_toggle.setEnabled(False)

        save_csv = self.auto_toggle.isChecked()
        self.migration_worker = MigrationWorker(dbo, tbl, q, save_csv)
        self.migration_worker.progress.connect(self.on_new_log_line)
        self.migration_worker.done.connect(self._migration_done)
        self.migration_worker.failed.connect(self._migration_failed)
        self.migration_worker.start()

    def _migration_done(self, df):
        self.last_df = df
        self.export_btn.setEnabled(df is not None)
        append_colored(self.log, "[SUCCESS] Migration completed and DataFrame returned.\n", "#67e667")
        QtWidgets.QMessageBox.information(self, "Migration Completed", "Data migration completed successfully.")
        self._restore_after_run()

    def _migration_failed(self, err):
        append_colored(self.log, f"[ERROR] Migration failed: {err}\n", "#ff6b6b")
        QtWidgets.QMessageBox.critical(self, "Migration Failed", f"Migration failed:\n{err}")
        self._restore_after_run()

    def _restore_after_run(self):
        self.start_btn.setEnabled(True)
        self.start_btn.setText("Start Migration")
        self.auto_toggle.setEnabled(True)

    def export_csv(self):
        if self.last_df is None:
            QtWidgets.QMessageBox.warning(self, "No Data", "No data available to export. Run migration first.")
            return
        if pd is None:
            QtWidgets.QMessageBox.warning(self, "Dependency Missing", "pandas not installed; cannot export.")
            return
        fn, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Export to CSV", str(Path.home() / "export.csv"), "CSV Files (*.csv)")
        if fn:
            try:
                self.last_df.to_csv(fn, index=False, encoding="utf-8-sig")
                QtWidgets.QMessageBox.information(self, "Exported", f"Data exported to:\n{fn}")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Export Failed", f"Could not export CSV:\n{e}")

    def report_issue(self):
        logp = Path(LOG_FILE).absolute()
        msg = (
            "To report an issue, please send an email to:\n\n"
            f"{REPORT_EMAIL}\n\n"
            "Attach the log file located at:\n\n"
            f"{logp}\n\n"
            "Provide steps to reproduce and any screenshots. The log folder will be opened now."
        )
        QtWidgets.QMessageBox.information(self, "Report Issue — Instructions", msg)
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(logp.parent))
            elif sys.platform.startswith("darwin"):
                os.system(f'open \"{logp.parent}\"')
            else:
                os.system(f'xdg-open \"{logp.parent}\"')
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Open Folder Failed", f"Could not open log folder: {e}")
            
            
    def show_shortcuts_dialog(self):
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Keyboard Shortcuts")
        dlg.setMinimumWidth(420)

        layout = QtWidgets.QVBoxLayout(dlg)
        title = QtWidgets.QLabel("Keyboard Shortcuts")
        title.setFont(QtGui.QFont("Segoe UI Semibold", 12))
        layout.addWidget(title)

        # Shortcut list (matches screenshot minimal style)
        shortcuts = [
            ("Run Migration",          "Ctrl + Enter"),
            ("Export CSV",             "Ctrl + S"),
            ("SQL Autocomplete",       "Ctrl + Space"),
            ("SQL Snippet (Tab)",      "Tab"),
            ("Format SQL",             "Ctrl + Shift + F"),
            ("Move Line Up/Down",      "Alt + ↑ / Alt + ↓   (future)"),
            ("Duplicate Line",         "Ctrl + D   (future)"),
        ]

        grid = QtWidgets.QGridLayout()
        grid.setVerticalSpacing(8)
        row = 0
        for label, key in shortcuts:
            lbl = QtWidgets.QLabel(label)
            ky = QtWidgets.QLabel(key)
            ky.setStyleSheet("color:#7d7d7d;")  # subtle like screenshot
            grid.addWidget(lbl, row, 0)
            grid.addWidget(ky, row, 1)
            row += 1

        layout.addLayout(grid)

        # Close button
        btn_close = QtWidgets.QPushButton("Close")
        btn_close.clicked.connect(dlg.close)
        btn_close.setFixedWidth(100)
        btn_close.setStyleSheet("margin-top:12px;")
        layout.addWidget(btn_close, alignment=QtCore.Qt.AlignmentFlag.AlignRight)

        dlg.exec()
        
        
    def load_theme_icon(self, name, size=16):
        folder = "dark" if self.dark_theme else "light"
        path = BASE / "icons" / folder / f"{name}.svg"
        if path.exists():
            r = QtSvg.QSvgRenderer(str(path))
            pix = QtGui.QPixmap(size, size)
            pix.fill(QtCore.Qt.GlobalColor.transparent)
            painter = QtGui.QPainter(pix)
            r.render(painter)
            painter.end()
            return QtGui.QIcon(pix)
        return QtGui.QIcon()



    # ---------------- validation & format ----------------
    def validate_query(self, q: str) -> bool:
        ql = q.strip().lower()
        if not ql.startswith("select"):
            QtWidgets.QMessageBox.critical(self, "Invalid Query", "Only SELECT statements are allowed for migration.")
            return False
        forbidden = ["insert","update","delete","drop","alter","truncate","merge","create","exec","grant","revoke"]
        for fwd in forbidden:
            if f"{fwd} " in ql or f"{fwd}(" in ql:
                QtWidgets.QMessageBox.critical(self, "Unsafe Query Detected", f"The query contains a potentially dangerous SQL command: '{fwd.upper()}'. Only read-only SELECT queries are allowed.")
                return False
        if "from" not in ql:
            QtWidgets.QMessageBox.critical(self, "Malformed Query", "Query seems invalid — missing a FROM clause.")
            return False
        if sqlparse:
            try:
                parsed = sqlparse.parse(q)
                if not parsed:
                    raise ValueError("Unrecognized SQL")
            except Exception as e:
                self.underline_error_all()
                QtWidgets.QMessageBox.critical(self, "SQL Syntax Error", f"SQL syntax appears invalid:\n{e}")
                return False
        self.clear_error_underlines()
        return True

    def underline_error_all(self):
        doc = self.editor.document()
        cursor = QtGui.QTextCursor(doc)
        cursor.select(QtGui.QTextCursor.SelectionType.Document)
        fmt = QtGui.QTextCharFormat()
        try:
            fmt.setUnderlineStyle(QtGui.QTextCharFormat.UnderlineStyle.WaveUnderline)
        except Exception:
            fmt.setUnderlineStyle(QtGui.QTextCharFormat.UnderlineStyle.SingleUnderline)
        fmt.setUnderlineColor(QtGui.QColor("#ff6b6b"))
        cursor.setCharFormat(fmt)

    def clear_error_underlines(self):
        doc = self.editor.document()
        cursor = QtGui.QTextCursor(doc)
        cursor.select(QtGui.QTextCursor.SelectionType.Document)
        cursor.setCharFormat(QtGui.QTextCharFormat())

    def format_sql(self):
        if not sql_format:
            QtWidgets.QMessageBox.warning(self, "Format SQL", "sqlparse is not installed; cannot format.")
            return
        raw = self.editor.toPlainText()
        try:
            formatted = sql_format(raw, reindent=True, keyword_case='upper')
            self.editor.setPlainText(formatted)
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Format Error", f"Could not format SQL:\n{e}")

    def closeEvent(self, ev):
        try:
            if hasattr(self, "logtail") and self.logtail.isRunning():
                self.logtail.stop()
                self.logtail.wait(1000)
        except:
            pass
        super().closeEvent(ev)

# ---------------- run ----------------
def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MigrationWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
