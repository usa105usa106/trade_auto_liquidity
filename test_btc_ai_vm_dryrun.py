import asyncio, time, os, math, json
from pathlib import Path
from ai_btc_autopilot import BTCVisionAutopilot, BTCAutopilotDecision
from execution_engine import ExecutionEngine
from position_manager import PositionManager

class MemStorage:
    def __init__(self):
        self._settings={
            'live_trading': True,
            'btc_ai_autopilot_enabled': True,
            'btc_ai_balance_share': 0.10,
            'btc_ai_leverage': 10,
            'btc_ai_min_trade_probability': 75,
            'protection_position_wait_sec': 0.01,
            'protection_position_poll_sec': 0.01,
            'limit_timeout_sec': 14400,
        }
        self._pos=[]
        self._locks={}
        self._trades=[]
    async def all_settings(self): return dict(self._settings)
    async def get(self,k,d=None): return self._settings.get(k,d)
    async def set(self,k,v,bump_revision=False): self._settings[k]=v
    async def positions(self): return [dict(p) for p in self._pos]
    async def position_symbols(self): return [p['symbol'] for p in self._pos if p.get('status') in {'open','pending','closing'}]
    async def is_locked(self,symbol): return (False,'')
    async def set_lock(self,*a,**k): pass
    async def upsert_position(self,pos):
        for i,p in enumerate(self._pos):
            if p.get('symbol')==pos.get('symbol'):
                self._pos[i]=dict(pos); return
        self._pos.append(dict(pos))
    async def remove_position(self,symbol):
        self._pos=[p for p in self._pos if p.get('symbol')!=symbol]
    async def increment_counter(self,*a,**k): return 1
    async def trade_rows(self, since=0): return self._trades

class FakeMexc:
    exchange_id='mexc'
    def __init__(self):
        self.orders=[]; self.positions=[]; self.last=100000.0; self.next_id=1
    def _id(self,prefix):
        self.next_id+=1; return f'{prefix}_{self.next_id}'
    def mexc_contract_symbol(self,s): return 'BTC_USDT'
    def mexc_symbol_variants(self,s): return ['BTC_USDT','BTC/USDT:USDT']
    def _mexc_symbol(self,s): return 'BTC_USDT'
    async def fetch_balance(self): return {'USDT': {'total': 18.0}, 'total': {'USDT': 18.0}}
    async def fetch_positions(self, symbols=None): return [dict(p) for p in self.positions]
    async def fetch_open_orders(self,symbol): return [dict(o) for o in self.orders if o.get('status')=='open']
    async def create_order(self, symbol, type_, side, amount, price=None, params=None):
        params=params or {}; oid=self._id('entry' if type_ in {'market','limit'} else 'order')
        order={'id':oid,'symbol':symbol,'type':type_,'side':side,'amount':amount,'filled':0,'price':price or self.last,'average':price or self.last,'status':'open','params':params}
        if type_=='market':
            order['filled']=amount; order['status']='closed'; order['average']=self.last
            pos_side='LONG' if side=='buy' else 'SHORT'
            self.positions=[{'id':'pos_1','symbol':symbol,'side':pos_side,'contracts':amount,'entryPrice':self.last,'info': {'positionId':'pos_1','holdVol': amount, 'holdAvgPrice': self.last}}]
        self.orders.append(order)
        return order
    async def fetch_order(self, oid, symbol):
        for o in self.orders:
            if o['id']==oid: return dict(o)
        return {'id': oid, 'status':'closed','filled':0,'amount':0,'average': self.last}
    async def cancel_order(self, oid, symbol):
        for o in self.orders:
            if o['id']==oid: o['status']='canceled'
        return {'ok': True, 'id': oid}
    async def cancel_all_orders(self, symbol):
        for o in self.orders: o['status']='canceled'
        return {'ok': True}
    async def fetch_ticker(self,symbol): return {'last': self.last}
    async def fetch_order_book(self,symbol,limit=50): return {'bids': [[99900,0.1]], 'asks': [[100100,0.1]]}
    async def fetch_ohlcv(self,symbol,timeframe='4h',limit=160):
        out=[]; base=100000; ts=int((time.time()-limit*14400)*1000)
        for i in range(limit):
            c=base + i*5 + math.sin(i/5)*300; o=c-20; h=max(o,c)+100; l=min(o,c)-100; v=100+i
            out.append([ts+i*14400*1000,o,h,l,c,v])
        self.last=out[-1][4]
        return out
    async def _mexc_public(self, method, path, query=None):
        if 'funding_rate' in path: return {'data': {'fundingRate': '0.0001','nextSettleTime': 0}}
        return {'data': {'holdVol': 12345, 'riseFallRate': '0.01'}}
    async def mexc_place_take_profit_market(self, symbol, close_side, amount, trigger_price, client_order_id=None, leverage=None):
        oid=self._id('tp'); self.orders.append({'id':oid,'symbol':symbol,'type':'plan_tp','side':close_side,'amount':amount,'price':trigger_price,'status':'open','clientOrderId':client_order_id}); return {'id':oid,'kind':'tp'}
    async def mexc_place_stop_market(self, symbol, close_side, amount, trigger_price, client_order_id=None, leverage=None):
        oid=self._id('sl'); self.orders.append({'id':oid,'symbol':symbol,'type':'plan_sl','side':close_side,'amount':amount,'price':trigger_price,'status':'open','clientOrderId':client_order_id}); return {'id':oid,'kind':'sl'}
    async def mexc_find_active_plan_order(self, symbol, order_id):
        for o in self.orders:
            if o['id']==order_id and o['status']=='open': return dict(o)
        return None

class FakeBot:
    async def send_message(self, chat_id, text): print('MSG:', text[:160].replace('\n',' | '))
    async def send_photo(self, chat_id, photo, caption): print('PHOTO:', caption[:160].replace('\n',' | '))
class FakeApp: bot=FakeBot()

async def main():
    os.environ['ADMIN_IDS']='1'
    st=MemStorage(); ex=FakeMexc(); ee=ExecutionEngine(st, ex); ai=BTCVisionAutopilot(st, ex, ee)
    candles=await ex.fetch_ohlcv('BTC_USDT', limit=160)
    md=await ai.collect_market_data('BTC_USDT', candles)
    # override binance fetched if external internet unavailable
    if isinstance(md.get('binance_spot_pressure'), dict) and md['binance_spot_pressure'].get('error'):
        md['binance_spot_pressure']={'buy_usdt':600000,'sell_usdt':400000,'delta_usdt':200000,'buy_ratio':0.6,'window_min':15}
        md['cross_exchange_pressure_normalized']=ai._normalize_cross_exchange_pressure(md['binance_spot_pressure'], md['mexc_volume_ratio_30'])
    p=ai.render_chart('BTC_USDT', candles, md); print('chart_exists', Path(p).exists(), Path(p).stat().st_size, p)
    d=BTCAutopilotDecision(signal='LONG', probability=86, grade='A+', entry_zone_low=ex.last*0.998, entry_zone_high=ex.last*1.001, stop_loss=ex.last*0.997, reason='dryrun')
    lv=ai.prepare_levels(d, md, forced_entry=ex.last)
    print('levels', json.dumps(lv, indent=2))
    ap=ai.render_signal_chart('BTC_USDT', candles, md, d, lv); print('annotated_exists', Path(ap).exists(), Path(ap).stat().st_size, ap)
    await ai.execute_decision(FakeApp(), await st.all_settings(), 'BTC_USDT', d, md, lv)
    print('positions_after_market', json.dumps(await st.positions(), indent=2, default=str)[:1000])
    print('orders_after_market', json.dumps(ex.orders, indent=2, default=str)[:2000])
    assert any(o['type']=='plan_tp' for o in ex.orders), 'no TP plan order created'
    assert any(o['type']=='plan_sl' for o in ex.orders), 'no SL plan order created'
    # Test limit placement -> fill -> protection attach
    st2=MemStorage(); ex2=FakeMexc(); ee2=ExecutionEngine(st2, ex2); ai2=BTCVisionAutopilot(st2, ex2, ee2)
    candles2=await ex2.fetch_ohlcv('BTC_USDT', limit=160)
    md2=await ai2.collect_market_data('BTC_USDT', candles2); md2['binance_spot_pressure']={'buy_usdt':600000,'sell_usdt':400000,'delta_usdt':200000,'buy_ratio':0.6,'window_min':15}; md2['cross_exchange_pressure_normalized']=ai2._normalize_cross_exchange_pressure(md2['binance_spot_pressure'], md2['mexc_volume_ratio_30'])
    d2=BTCAutopilotDecision(signal='LONG', probability=80, grade='A', entry_zone_low=ex2.last*0.998, entry_zone_high=ex2.last*1.001, stop_loss=ex2.last*0.997, reason='dryrun')
    lv2=ai2.prepare_levels(d2, md2)
    await ai2.execute_decision(FakeApp(), await st2.all_settings(), 'BTC_USDT', d2, md2, lv2)
    pos=(await st2.positions())[0]; print('limit_pending', pos['status'], pos['order_id'])
    # simulate limit fill
    for o in ex2.orders:
        if o['id']==pos['order_id']:
            o['status']='closed'; o['filled']=o['amount']; o['average']=pos['entry_price']; ex2.positions=[{'id':'pos_1','symbol':'BTC_USDT','side':'LONG','contracts':pos['qty'],'entryPrice':pos['entry_price'],'info': {'positionId':'pos_1','holdVol': pos['qty'], 'holdAvgPrice': pos['entry_price']}}]
    pm=PositionManager(st2, ee2)
    ev=await pm._manage_pending(pos, live=True); print('pending_fill_event', ev)
    print('orders_after_limit_fill', json.dumps(ex2.orders, indent=2, default=str)[:2000])
    assert any(o['type']=='plan_tp' for o in ex2.orders), 'limit fill no TP'
    assert any(o['type']=='plan_sl' for o in ex2.orders), 'limit fill no SL'
asyncio.run(main())
