#!./venv/bin/python
import argparse
import datetime
from pubsub import pub
import sqlite3
import sys
import threading
import time
import wx
import wx.dataview as wxdv
import wx.lib.newevent

cr_conf = __import__('cr-conf')
cf = cr_conf.conf
cr_report = __import__('cr-report')
cr_update = __import__('cr-update')


_ffs = set()


def pub_subscribe(listener, topic):
    _ffs.add(listener)
    return pub.subscribe(listener, topic)


class AbortUpdateException(Exception):
    pass


def emit_update_started():
    pub.sendMessage('update-started')


def emit_update_status(section, message):
    pub.sendMessage('update-status', section=section, message=message)


def emit_update_rebuilt(success, raider, reason=None):
    pub.sendMessage('update-rebuilt',
                    success=success, raider=raider, reason=reason)


def emit_update_complete(success, raider):
    pub.sendMessage('update-complete', success=success, raider=raider)


def emit_raider_selected(rid=None):
    assert rid is None or isinstance(rid, int)
    pub.sendMessage('raider-selected', raider=rid)


def emit_reload_database():
    pub.sendMessage('reload-database')


class UpdaterThread(threading.Thread):
    def __init__(self, getdb, notify_window, raider_ids=None):
        super().__init__()
        self.__getdb = getdb
        self.__notify = notify_window
        self.__raiders = raider_ids
        self.__abort = False
        self.__status_section = None

    def progress_callback(self, section=None, message=None):
        emit_status = False
        if section is None:
            section = self.__status_section
        elif section != self.__status_section:
            self.__status_section = section
            emit_status = True
        if message is None:
            message = ''
        else:
            emit_status = True

        if emit_status:
            wx.CallAfter(emit_update_status, section, message)
        if self.__abort:
            raise AbortUpdateException()

    def run(self):
        success = False
        reason = None
        db = self.__getdb()
        try:
            cr_update.import_or_update(db, raider=self.__raiders,
                                       periodic=self.progress_callback)
            success = True
        except AbortUpdateException:
            db.rollback()
            reason = 'canceled by user'
        except Exception as ex:
            db.rollback()
            reason = ex
        wx.CallAfter(emit_update_rebuilt, success, self.__raiders, reason)

    def abort_update(self):
        self.__abort = True


class RaidCountRenderer(wxdv.DataViewTextRenderer):
    def SetValue(self, val):
        return super().SetValue('' if val < 0 else val)


class TimeDeltaRenderer(wxdv.DataViewTextRenderer):
    def SetValue(self, val):
        if val < 0:
            strval = ''
        elif val == 0:
            strval = 'now'
        else:
            strval = str(datetime.timedelta(seconds=int(val)))
        return super().SetValue(strval)


class DateTimeRenderer(wxdv.DataViewTextRenderer):
    def SetValue(self, val):
        if val < 0:
            strval = ''
        elif val == 0:
            strval = 'now'
        else:
            delta = val - time.time()
            strval = str(datetime.timedelta(seconds=int(delta)))
        return super().SetValue(strval)


class Float1Renderer(wxdv.DataViewTextRenderer):
    def SetValue(self, val):
        strval = '' if val is None else '%.1f' % (val,)
        return super().SetValue(strval)


class RaiderPage():
    def __init__(self, notebook, state):
        super().__init__()
        self.notebook = notebook
        self.state = state
        self._col_types = {
            'str': ('string', wxdv.DataViewTextRenderer),
            'int': ('long', wxdv.DataViewTextRenderer),
            'float_1': ('double', Float1Renderer),
            'positive_count': ('long', RaidCountRenderer),
            'delta_seconds': ('long', TimeDeltaRenderer),
            'epoch_seconds': ('long', DateTimeRenderer),
        }
        pub_subscribe(self.refresh, 'reload-database')
        pub_subscribe(self.__update_started, 'update-started')
        pub_subscribe(self.__update_complete, 'update-complete')

    def __update_started(self):
        self.view.Disable()

    def __update_complete(self, success, raider):
        self.view.Enable(True)

    def refresh(self):
        pass

    def add_view_columns(self, view, sortable=True, lastfake=False):
        for idx, colname in enumerate(self.report.columns):
            label = self.report.labels[idx]
            align = (wx.ALIGN_RIGHT if self.report.right_align[idx]
                     else wx.ALIGN_LEFT)
            assert self.report.coltypes[idx] in self._col_types
            typstr, rendcls = self._col_types.get(self.report.coltypes[idx])
            rend = rendcls(typstr)
            col = wxdv.DataViewColumn(label, rend, idx, align=align,
                                      width=wx.COL_WIDTH_AUTOSIZE)
            view.AppendColumn(col)
            col.SetSortable(sortable)

        if lastfake:
            view.AppendColumn(wxdv.DataViewColumn(
                '', wxdv.DataViewTextRenderer('string'),
                idx + 1, width=wx.COL_WIDTH_AUTOSIZE))


class SingleRaiderPage(RaiderPage):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        pub_subscribe(self.set_raider, 'raider-selected')

    def set_raider(self, raider):
        self.refresh()


class ListTab(RaiderPage):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.report = cr_report.RaiderListReport()
        self.rid_idx = 0
        self.reloading_data = False

        panel = wx.Panel(self.notebook)
        sizer = wx.BoxSizer(wx.VERTICAL)
        self.view = wxdv.DataViewListCtrl(panel)
        sizer.Add(self.view, proportion=1, flag=wx.EXPAND)
        self.notebook.AddPage(panel, 'Raiders')

        self.add_view_columns(self.view)
        self.view.GetColumn(self.rid_idx).SetSortOrder(True)
        self.view.Bind(wxdv.EVT_DATAVIEW_SELECTION_CHANGED,
                       self.selection_handler)
        pub_subscribe(self.select_raider, 'raider-selected')

        self.refresh()

        panel.SetSizerAndFit(sizer)

    def get_selected_raider(self):
        row = self.view.GetSelectedRow()
        if row != wx.NOT_FOUND:
            return self.view.GetValue(row, self.rid_idx)

    def select_raider(self, raider):
        old_raider = self.get_selected_raider()
        if old_raider == raider:
            return

        if raider is None:
            self.view.SelectRow(wx.NOT_FOUND)
            return

        for idx in range(self.view.GetItemCount()):
            rid = self.view.GetValue(idx, self.rid_idx)
            if rid == raider:
                self.view.SelectRow(idx)
                return

    def selection_handler(self, ev):
        if self.state.updating or self.reloading_data:
            return
        item = ev.GetItem()
        if item.IsOk():
            rid = ev.GetModel().GetValue(item, 0)
            self.state.select_raider(rid)
        else:
            self.state.select_raider()

    def refresh(self):
        assert not self.state.updating
        try:
            self.reloading_data = True
            old_raider = self.state.raider
            self.view.DeleteAllItems()
            for row in self.report.fetch(self.state.db):
                self.view.AppendItem(row)
            self.select_raider(old_raider)
        finally:
            self.reloading_data = False


class FormattedListStore(wxdv.DataViewListStore):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.__fmt = {}
        self.__plus = wx.Colour(0, 255, 0)
        self.__minus = wx.Colour(255, 0, 0)

    def getattr_delta(self, val, attr):
        if isinstance(val, float) or isinstance(val, int):
            if val > 0.005:
                attr.SetBold(True)
                attr.SetColour(self.__plus)
                return True
            elif val < -0.005:
                attr.SetBold(True)
                attr.SetColour(self.__minus)
                return True
        return False

    def append_row(self, row, format=None):
        if format:
            self.__fmt[self.GetItemCount()] = format
        self.AppendItem(row)

    def DeleteAllItems(self):
        self.__fmt.clear()
        return super().DeleteAllItems()

    def GetAttrByRow(self, row, col, attr):
        fmt = self.__fmt.get(row)
        if fmt == 'bold':
            attr.SetBold(True)
            return True
        elif fmt == 'italic':
            attr.SetItalic(True)
            return True
        elif fmt == 'delta':
            return self.getattr_delta(self.GetValueByRow(row, col), attr)
        return False


class GearListStore(FormattedListStore):
    def GetAttrByRow(self, row, col, attr):
        return self.getattr_delta(self.GetValueByRow(row, col), attr)


class GearTab(SingleRaiderPage):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.report = cr_report.RaiderGearReport()
        self.view = wxdv.DataViewCtrl(self.notebook,
                                      style=wxdv.DV_HORIZ_RULES)
        self.store = GearListStore()
        self.view.AssociateModel(self.store)
        self.notebook.AddPage(self.view, 'Gear')

        self.add_view_columns(self.view, lastfake=True)
        self.view.GetColumn(0).SetSortOrder(True)

    def refresh(self):
        assert not self.state.updating
        self.store.DeleteAllItems()
        if self.state.raider is not None:
            for row in self.report.fetch(self.state.db, self.state.raider):
                self.store.AppendItem(row + ('',))


class ComboListStore(FormattedListStore):
    def load_report(self, report, db, raider):
        self.DeleteAllItems()
        if raider is None:
            return

        equipped, combos = list(report.fetch_more(db, raider))
        for combo_row, diff_row, weap_row, dress_row, ring_row in combos[:100]:
            cols = len(combo_row)
            for row in (weap_row, dress_row, ring_row):
                floats = [float(i) for i in (row[1:] + (0,) * cols)[:cols-1]]
                new_row = (row[0],) + tuple(floats) + ('',)
                if row in equipped:
                    self.append_row(new_row, format='bold')
                else:
                    self.append_row(new_row)
            new_combo_row = ('Total',) + combo_row[1:] + ('',)
            self.append_row(new_combo_row, format='italic')
            self.append_row(diff_row + ('',), format='delta')


class ComboTab(SingleRaiderPage):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.report = cr_report.RaiderComboReport()
        self.view = wxdv.DataViewCtrl(self.notebook)
        self.store = ComboListStore()
        self.view.AssociateModel(self.store)
        self.notebook.AddPage(self.view, 'Combos')
        self.add_view_columns(self.view, sortable=False, lastfake=True)

    def refresh(self):
        assert not self.state.updating
        self.store.load_report(self.report, self.state.db,
                               self.state.raider)


class RaiderToolbar(wx.ToolBar):
    def __init__(self, state):
        style = wx.TB_BOTTOM | wx.TB_TEXT | wx.TB_NOICONS
        super().__init__(state, style=style)
        self.id_update_all = 1
        self.id_update_one = 2
        self.id_cancel = 3
        self.state = state

        for tid, label, handler in (
                (self.id_update_all, 'Update All', self.__tool_update_all),
                (self.id_update_one, 'Update One', self.__tool_update_one),
                (self.id_cancel, 'Cancel', self.__tool_cancel)):
            self.Bind(wx.EVT_TOOL, handler,
                      self.AddTool(tid, label, wx.NullBitmap))
        self.Realize()
        self.enable_tools()

        pub_subscribe(self.__raider_selected, 'raider-selected')
        pub_subscribe(self.__update_started, 'update-started')
        pub_subscribe(self.__update_complete, 'update-complete')
        state.SetToolBar(self)

    def __raider_selected(self, raider):
        self.enable_tools()

    def __update_started(self):
        self.enable_tools()

    def __update_complete(self, success, raider):
        self.enable_tools()

    def __tool_update_all(self, ev):
        if not self.state.updating:
            self.state.update()

    def __tool_update_one(self, ev):
        if not self.state.updating and self.state.raider is not None:
            self.state.update(self.state.raider)

    def __tool_cancel(self, ev):
        if self.state.updating:
            self.state.updating.abort_update()

    def enable_tools(self):
        updating = self.state.updating is not None
        selected = self.state.raider is not None
        self.EnableTool(self.id_update_all, not updating)
        self.EnableTool(self.id_update_one, (not updating) and selected)
        self.EnableTool(self.id_cancel, updating)


class CRUtilRoot(wx.Frame):
    def __init__(self, parent, dbpath, **kw):
        super().__init__(parent, **kw)
        self.getdb = lambda: sqlite3.connect(dbpath)
        self.db = self.getdb()
        self.raider = None
        self.updating = None
        self.CreateStatusBar().SetFieldsCount(2)
        RaiderToolbar(self)

        pub_subscribe(self.handle_update_rebuilt, 'update-rebuilt')
        pub_subscribe(self.set_status_text, 'update-status')

        self.notebook = wx.Notebook(self)
        ListTab(self.notebook, self)
        GearTab(self.notebook, self)
        ComboTab(self.notebook, self)

    def select_raider(self, raider=None):
        assert raider is None or isinstance(raider, int)
        if raider != self.raider:
            self.raider = raider
            emit_raider_selected(raider)

    def update(self, raider=None):
        if self.updating is None:
            self.updating = UpdaterThread(self.getdb, self, raider)
            emit_update_started()
            self.updating.start()

    def handle_update_rebuilt(self, success, raider, reason):
        self.updating = None
        if not success:
            self.set_status_text('Update failed', str(reason))
        else:
            self.set_status_text('Update complete')
            emit_reload_database()
            emit_raider_selected(raider)
        emit_update_complete(success, raider)

    def set_status_text(self, section, message=''):
        bar = self.GetStatusBar()
        if bar:
            bar.SetStatusText(section, 0)
            bar.SetStatusText(message, 1)


def main():
    parser = argparse.ArgumentParser()
    parser.parse_args()

    if not cf.load_config():
        print('error: please run ./cr-conf.py to configure')
        sys.exit(1)

    app = wx.App()
    root = CRUtilRoot(None, cf.db_path, title='Crypto Raiders Utility')
    root.Show()
    app.MainLoop()


if __name__ == '__main__':
    main()
