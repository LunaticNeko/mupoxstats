from pox.core import core
from pox.lib.revent import *
import pox.openflow.libopenflow_01 as of
import pox.lib.util as poxutil
from pox.lib.util import dpid_to_str
import pox.lib.packet as pkt
import pox.topology as topo
import pox.openflow.discovery as discovery
import networkx as nx
import threading
from time import sleep
import time
import datetime
import sqlite3
from types import *
import sqlalchemy.pool

#import whatever we need from those pox modules

log = core.getLogger()

ping_timer = 2

class Stats(EventMixin):
    _core_name = "stats"

    G = nx.DiGraph()

    pool = sqlalchemy.pool.manage(sqlite3, poolclass=sqlalchemy.pool.StaticPool)
    #db = pool.connect(':memory:')
    db = sqlite3.connect(':memory:', check_same_thread=False)

    #### DB DEFINITION

    cur = db.cursor()

    def _db_init(self):
        self.cur.execute('''
                CREATE TABLE statsdata(
                    dpid integer,
                    port integer,
                    time integer not null,
                    rxb integer,
                    rxe integer,
                    rxd integer,
                    txb integer,
                    txe integer,
                    txd integer,
                    dtime integer,
                    drxb integer,
                    drxe integer,
                    drxd integer,
                    dtxb integer,
                    dtxe integer,
                    dtxd integer,
                    primary key (dpid, port)
                );
                ''')
        log.info('Database Initialized')
        log.info(self.db)

    def _db_record(self, dpid, port, time, rxb, rxe, rxd, txb, txe, txd):
        assert type(dpid) is IntType, "dpid must be integer"
        assert type(port) is IntType, "port must be integer"
        assert type(rxb) is IntType, "rxb must be integer"
        assert type(rxe) is IntType, "rxe must be integer"
        assert type(rxd) is IntType, "rxd must be integer"
        assert type(txb) is IntType, "txb must be integer"
        assert type(txe) is IntType, "txe must be integer"
        assert type(txd) is IntType, "txd must be integer"
        #db = self.pool.connect(':memory:')
        cur = self.db.cursor()
        cur.execute('''
            SELECT * FROM statsdata WHERE dpid=? and port=?
        ''', (dpid, port))
        existing_rows = cur.fetchall()
        if len(existing_rows) == 0:
            cur.execute('''
                INSERT INTO statsdata VALUES (?,?,?,?,?,?,?,?,?,0,0,0,0,0,0,0)
            ''', (dpid, port, time, rxb, rxe, rxd, txb, txe, txd))
        else:
            cur.execute('''
                UPDATE statsdata SET
                    time = ?, rxb = ?, rxe = ?, rxd = ?, txb = ?, txe = ?, txd = ?,
                    dtime = time, drxb = rxb, drxe = rxe, drxd = rxd, dtxb = txb, dtxe = txe, dtxd = txd
                WHERE dpid = ? AND port = ?
            ''', (time, rxb, rxe, rxd, txb, txe, txd, dpid, port))

    def _db_get(self, dpid, port=None):
        assert type(dpid) is IntType, "dpid must be integer"
        if port is not None:
            assert type(port) is IntType, "port must be integer"
        cur = self.db.cursor()
        if port is None:
            cur.execute('''SELECT * FROM statsdata WHERE dpid=?''', (dpid))
        else:
            cur.execute('''SELECT * FROM statsdata WHERE dpid=? AND port=?''', (dpid, port))
        rows = cur.fetchall()
        return rows

    #### END DB DEFINITION

    live_data = dict()
    diff_data = dict()

    def __init__(self):
        core.listen_to_dependencies(self)
        G = nx.Graph()
        self._db_init()
        ping_thread = threading.Thread(target = self._start_ping_switches)
        ping_thread.daemon = True
        ping_thread.start()

    def _start_ping_switches(self):
        while True:
            self._ping_switches()
            sleep(10)

    def _ping_switches(self):
        log.info("Pinging Switches")
        nodes = self.G.nodes()
        for dpid in nodes:
            con = core.openflow.getConnection(dpid)
            #log.info("Pinging %x" % (dpid))
            #log.info((con, con.connect_time))
            msg = of.ofp_stats_request()
            msg.body = of.ofp_port_stats_request()
            if con is not None and con.connect_time is not None:
                con.send(msg)

    def _handle_openflow_ConnectionUp(self, event):
        log.info("Up: %x" % (event.dpid))
        self.G.add_node(event.dpid)

    def _handle_openflow_ConnectionDown(self, event):
        log.info("Down: %x" % (event.dpid))
        self.G.remove_node(event.dpid)

    def _handle_openflow_FeaturesReceived(self, event):
        #log.info(event)
        pass

    def _handle_openflow_FlowStatsReceived(self, event):
        log.info(event)

    def _handle_openflow_FlowRemoved(self, event):
        log.info(event)

    def _handle_openflow_PortStatsReceived(self, event):
        log.info(event)
        log.info((dpid_to_str(event.dpid), str(datetime.datetime.now())))
        unixtime = int(time.mktime(datetime.datetime.now().timetuple()))
        if len(event.stats)>0:
            for stat in event.stats:
                log.info("  %d: RX %d B/%d E/%d D; TX %d B/%d E/%d D",
                        stat.port_no,
                        stat.rx_bytes,
                        stat.rx_errors,
                        stat.rx_dropped,
                        stat.tx_bytes,
                        stat.tx_errors,
                        stat.tx_dropped
                        )
                self._db_record(
                        event.dpid, stat.port_no, unixtime,
                        stat.rx_bytes, stat.rx_errors, stat.rx_dropped,
                        stat.tx_bytes, stat.tx_errors, stat.tx_dropped
                        )
                stat_row = self._db_get(event.dpid, stat.port_no)
                if len(stat_row) == 0:
                    log.warn("%x %d should have a db record. Haven't.", event.dpid, stat.port_no)
                    continue
                new_stat = stat_row[0][2:9]
                old_stat = stat_row[0][9:16]
                diff = [new_stat[i]-old_stat[i] for i in range(0,7)]
                log.info("   d: RX %d B/%d E/%d D; TX %d B/%d E/%d D",
                        diff[1],diff[2],diff[3],diff[4],diff[5],diff[6])



    def _handle_openflow_PacketIn(self, event):
        log.info(event)

    def _handle_openflow_discovery_LinkEvent(self, event):
        dpid1 = event.link.dpid1
        dpid2 = event.link.dpid2
        port1 = event.link.port1
        port2 = event.link.port2
        link = ((dpid1, port1), (dpid2, port2))
        link_info = ((dpid_to_str(dpid1), port1), (dpid_to_str(dpid2), port2))
        if event.added and not event.removed:
            self.G.add_edge(dpid1, dpid2)
            self.G.add_edge(dpid2, dpid1)
            log.info(("Added", link_info))
        if event.removed and not event.added:
            if self.G.has_edge(dpid1, dpid2):
                self.G.remove_edge(dpid1, dpid2)
            if self.G.has_edge(dpid2, dpid1):
                self.G.remove_edge(dpid2, dpid1)
            log.info(("Removed", link_info))


@poxutil.eval_args
def launch():
    core.listen_to_dependencies(['of_01', 'topology', 'libopenflow01'])
    core.registerNew(Stats)

