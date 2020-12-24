from config_read import config as con
from Database.Service.stock_service import stock_service
from Database.Service.database import database
from ta.trend import PSARIndicator
from Utility.data_mainpulation import lowest_value as get_low, highest_value as get_high
from statistics import stdev

# ADD IN ADDITIONAL INDICATORS USING THE LIBRARY BELOW
# https://technical-analysis-library-in-python.readthedocs.io/en/latest/index.html

class retrieve_indicator_data:
    def __init__(self):
        config = con()
        self.conn_stock = database().conn_stock
        self.np = config.process_number
        # self.start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * 2)).strftime('%Y-%m-%d')
        self.sma_periods = config.sma_periods
        self.ema_periods = config.ema_periods
        self.ema_smoothing = config.ema_smoothing
        # self.list_of_stocks = stock_list().get_list_of_stocks()
        # self.list_of_stocks = [('GM',), ('AMZN',), ('NFLX',), ('AAPL',), ('GOOG',), ('MSFT',), ('SPCE',)]
        self.list_of_stocks = [('GM',)]

    # INDEPENDENT
    def sma(self, ticker, period, candle_value = 'adj_close'):
        data = stock_service().select_all_stock_objs(ticker)
        sma_dict = {
            'period' : period,
            'candle_value' : candle_value,
            'ticker' : ticker,
            'data' : []
        }
        for i, entry in enumerate(data):
            sma_init = 0
            if i > period:
                sma_init = sum([getattr(v, candle_value) for v in data[i - period + 1: i + 1]]) / period
            if sma_init > 0:
                sma_value = { 'date' : str(entry.date), 'value' : sma_init}
                sma_dict['data'].append(sma_value)
        return sma_dict

    # INDEPENDENT
    def ema(self, ticker, period, candle_value = 'close', smoothing = 2):
        data = stock_service().select_all_stock_objs(ticker)
        ema_dict = {
            'period': period,
            'candle_value': candle_value,
            'ticker': ticker,
            'data': []
        }
        ema_init = 0
        for i, entry in enumerate(data):
            if i > period + 1:
                if ema_init == 0:
                    ema_init = sum([getattr(v, candle_value) for v in data[i - period + 1: i + 1]]) / period
                multiplier = (smoothing / (period + 1))
                ema_value = (getattr(data[i], candle_value) - ema_init) * multiplier + ema_init
                ema_init = ema_value
                ema_dict['data'].append({ 'date' : str(entry.date), 'value' : ema_value})
        return ema_dict

    # INDEPENDENT
    def macd(self, ticker, fast_period = 12, slow_period = 26, signal_period = 9, candle_value = 'close', smoothing = 2):
        macd_dict = {
            'period': f"Fast: {fast_period}, Slow: {slow_period}",
            'candle_value': candle_value,
            'ticker': ticker,
            'data': []
        }
        fast_macd_line = self.ema(ticker, fast_period, candle_value, smoothing)
        slow_macd_line = self.ema(ticker, slow_period, candle_value, smoothing)

        signal, signal_init, i = 0, 0, 0
        d = slow_period - fast_period
        for i in range(len(fast_macd_line['data'])):
            if i > slow_period:
                if signal_init == 0:
                    signal = sum(v['macd'] for v in macd_dict['data'][i - signal_period + 1: i + 1]) / signal_period
                else:
                    multiplier = (smoothing / (signal_period + 1))
                    signal = ((fast_macd_line['data'][i]['value'] - slow_macd_line['data'][i-d]['value']) - signal_init) * multiplier + signal_init
                signal_init = signal
            macd_dict['data'].append({
                'date' : fast_macd_line['data'][i]['date'],
                'fast' : fast_macd_line['data'][i]['value'],
                'slow' : slow_macd_line['data'][i-d]['value'],
                'macd' : fast_macd_line['data'][i]['value'] - slow_macd_line['data'][i-d]['value'],
                'signal' : signal})
        return macd_dict

    # INDEPENDENT
    def stoch_ind(self, ticker, candle_value = 'close', slow_period = 14, fast_period = 3):
        data = stock_service().select_all_stock_objs(ticker)
        stoch_dict = {
            'period' : f"slow: {slow_period}, fast: {fast_period}",
            'candle_value' : candle_value,
            'ticker' : ticker,
            'data' : []
        }

        fast_stoch_value = 0
        for i, entry in enumerate(data):
            if i > slow_period:
                current_value = getattr(data[i], candle_value)
                lowest_value = get_low(data[i-slow_period+1:i+1], 'low')
                highest_value = get_high(data[i-slow_period+1:i+1], 'high')
                stoch_value = (current_value - lowest_value) / (highest_value - lowest_value) * 100
                if i > slow_period + fast_period:
                    fast_stoch_value = (stoch_value + stoch_dict['data'][i-slow_period-fast_period]['stoch_value'] +
                                        stoch_dict['data'][i-slow_period-fast_period+1]['stoch_value'])/fast_period
                stoch_dict['data'].append({
                    'date': entry.date,
                    'stoch_value' : stoch_value,
                    'fast_stoch_value' : fast_stoch_value
                })
        return stoch_dict

    # INDEPENDENT
    def rsi_ind(self, ticker, period = 14, candle_value = 'adj_close'):
        data = stock_service().select_all_stock_objs(ticker)
        rsi_dict = {
            'period': period,
            'candle_value': candle_value,
            'ticker': ticker,
            'data': []
        }

        rs = 0
        for i, entry in enumerate(data):
            gains, losses = [], []
            if i > period:
                # Spits out average gains and losses
                for x in range(i - period+1, i+1):
                    dif = getattr(data[x], candle_value) - getattr(data[x-1], candle_value)
                    if dif > 0:
                        gains.append(abs(dif))
                        losses.append(0)
                    if dif < 0:
                        gains.append(0)
                        losses.append(abs(dif))
                    if dif == 0:
                        gains.append(0)
                        losses.append(0)
                if rs == 0:
                    rs = (sum(gains)/len(gains))/(sum(losses)/len(losses))
                    avg_up_previous = sum(gains)/len(gains)
                    avg_down_previous = sum(losses)/len(losses)
                else:
                    avg_up = ((1/14) * gains[-1]) + ((13/14) * avg_up_previous)
                    avg_down = ((1/14) * losses[-1]) + ((13/14) * avg_down_previous)
                    rs = avg_up / avg_down
                    avg_up_previous = avg_up
                    avg_down_previous = avg_down
                rsi = 100 - (100/(1 + rs))
                rsi_dict['data'].append({
                    'date' : entry.date,
                    'rsi' : rsi
                })
        return rsi_dict

    # INDEPENDENT
    def willr_ind(self, ticker, period = 14, candle_value = 'close'):
        data = stock_service().select_all_stock_objs(ticker)
        willr_dict = {
            'period': period,
            'candle_value': candle_value,
            'ticker': ticker,
            'data': []
        }

        for i, entry in enumerate(data):
            if i > period:
                highest_high = get_high(data[i - period: i], 'high')
                lowest_low = get_low(data[i - period: i], 'low')
                willr = ((highest_high - getattr(data[i], candle_value)) / (highest_high - lowest_low)) * -100
                willr_dict['data'].append({
                    'date' : entry.date,
                    'willr_value' : willr
                })
        return willr_dict

    # INDEPENDENT
    def adx_ind(self, ticker, period = 14):
        data = stock_service().select_all_stock_objs(ticker)
        adx_dict = {
            'period': period,
            'ticker': ticker,
            'data': []
        }
        cDM_p, cDM_n, cTR = [], [], []
        DM_p14, DM_n14, TR_14 = [], [], []
        DM_plus, DM_minus, DM_diff, DM_sum, DX = [], [], [], [], []
        ADX = []
        for i, entry in enumerate(data):
            if i > 1:
                upM = data[i].high - data[i-1].high
                downM = data[i-1].low - data[i].low
                cTR.append(max((data[i].high - data[i].low), abs(data[i].high - data[i-1].close), abs(data[i].low - data[i-1].close)))
                if upM > downM:
                    cDM_p.append(max(upM, 0))
                    cDM_n.append(0)
                else:
                    cDM_n.append(max(downM, 0))
                    cDM_p.append(0)
                if i > period:
                    if len(DM_p14) == 0 or len(DM_n14) == 0 or len(TR_14) == 0:
                        DM_p14.append(sum(cDM_p[i-period:]))
                        DM_n14.append(sum(cDM_n[i-period:]))
                        TR_14.append(sum(cTR[i-period:]))
                    else:
                        DM_p14.append(DM_p14[-1] - (DM_p14[-1] / period) + cDM_p[-1])
                        DM_n14.append(DM_n14[-1] - (DM_n14[-1] / period) + cDM_n[-1])
                        TR_14.append(TR_14[-1] - (TR_14[-1] / period) + cTR[-1])
                    DM_plus.append(100 * (DM_p14[-1]/TR_14[-1]))
                    DM_minus.append(100 * (DM_n14[-1]/TR_14[-1]))
                    DM_diff.append(abs(DM_plus[-1] - DM_minus[-1]))
                    DM_sum.append(DM_plus[-1] + DM_minus[-1])
                    DX.append(100 * (DM_diff[-1]/DM_sum[-1]))
                    if i > period * 2:
                        if len(ADX) == 0:
                            ADX.append(sum(DX[i-period:])/period)
                        else:
                            ADX.append(((ADX[-1] * (period-1)) + DX[-1])/period)
                        adx_dict['data'].append({
                            'date' : entry.date,
                            'DM+' : DM_plus[-1],
                            'DM-' : DM_minus[-1],
                            'ADX' : ADX[-1]
                        })
        return adx_dict

    #INDEPENDENT
    def cci_ind(self, ticker, period = 20, use_adjust_close = False, smoothing = 0.015):
        data = stock_service().select_all_stock_objs(ticker)
        cci_dict = {
            'period': period,
            'ticker': ticker,
            'data': []
        }

        typical_price = []
        sma, mean_dev, cci = [], [], []
        for i, entry in enumerate(data):
            close = entry.close
            if use_adjust_close == True:
                close = entry.adj_close
            typical_price.append((entry.high + entry.low + close)/3)
            if i > period:
                sma.append(sum(typical_price[i-period + 1:])/period)
                mean_devC = 0
                for x in range(i - period + 1, i + 1):
                    mean_devC += abs(sma[-1] - typical_price[x])
                mean_dev.append(mean_devC/period)
                cci = (typical_price[-1] - sma[-1])/(smoothing * mean_dev[-1])
                cci_dict['data'].append({
                    'date' : entry.date,
                    'cci' : cci
                })
        return cci_dict

    #INDEPENDENT
    def aroon_ind(self, ticker, period = 25):
        data = stock_service().select_all_stock_objs(ticker)
        aroon_dict = {
            'period': period,
            'ticker': ticker,
            'data': []
        }

        for i, entry in enumerate(data):
            if i > period:
                highest_high = data[i].high
                lowest_low = data[i].low
                current_index = data.index(data[i])

                for x in range(i-period, i):
                    if data[x].high > highest_high:
                        highest_high_index = data.index(data[x])
                        highest_high = data[x].high
                    if data[x].low < lowest_low:
                        lowest_low_index = data.index(data[x])
                        lowest_low = data[x].low
                aroon_up = ((period - (current_index - highest_high_index)) / period) * 100
                aroon_down = ((period - (current_index - lowest_low_index)) / period) * 100
                aroon_dict['data'].append({
                    'date' : entry.date,
                    'aroon_up' : aroon_up,
                    'aroon_down' : aroon_down
                })
        return aroon_dict

    #INDEPENDENT
    def bbands_ind(self, ticker, period = 20, candle_value = 'close', standard_deviations = 2):
        data = stock_service().select_all_stock_objs(ticker)
        bbands_dict = {
            'period': period,
            'ticker': ticker,
            'data': []
        }
        for i, entry in enumerate(data):
            if i > period:
                typical_price = []
                ma = []
                for x in range(i-period + 1, i + 1):
                    ma.append(data[x].close)
                    typical_price.append((data[x].high + data[x].low + getattr(data[x], candle_value)) / 3)
                sma = sum(ma)/period
                stnd_dev = stdev(typical_price)
                upper_band = sma + stnd_dev * standard_deviations
                lower_band = sma - stnd_dev * standard_deviations
                band_width = upper_band - lower_band
                bbands_dict['data'].append({
                    'date' : entry.date,
                    'bb-mid' : sma,
                    'bb-high' : upper_band,
                    'bb-low' : lower_band,
                    'stnd_dev' : stnd_dev
                })
        return bbands_dict

    # INDEPENDENT
    def psar_ind(self, ticker, step = 0.020, max_step = 0.20, use_adj_close = False):
        close = 'close'
        if use_adj_close:
            close = 'adj_close'
        data = stock_service().select_all_stock_objs(ticker, dataframe=True, cleaned=False)
        ind_psar = PSARIndicator(high=data["high"], low=data["low"], close=data[close], step=step, max_step=max_step, fillna=True)
        data['psar_down'] = ind_psar.psar_down()
        data['psar_up'] = ind_psar.psar_up()
        data['psar_up_ind'] = ind_psar.psar_up_indicator()
        data['psar_down_ind'] = ind_psar.psar_down_indicator()
        data['psar'] = ind_psar.psar()

        psar_dict = {
            'ticker': ticker,
            'close_type' : close,
            'step' : step,
            'max_step' : max_step,
            'data': []
        }

        for i in range(0, data.shape[0]):
            row = data.iloc[i, :]

            psar_dict['data'].append({
                'date' : row.loc['dt'],
                'psar_down' : row.loc['psar_down'],
                'psar_up' : row.loc['psar_up'],
                'psar_down_ind' : row.loc['psar_down_ind'],
                'psar_up_ind' : row.loc['psar_up_ind'],
                'psar' : row.loc['psar']
            })
        return psar_dict


    def ad_ind(self, ticker, use_adj_close = False):
        data = stock_service().select_all_stock_objs(ticker)
        ad_dict = {
            'ticker': ticker,
            'data': []
        }
        close = 'close'
        if use_adj_close:
            close = 'adj_close'

        ad = []
        for i, entry in enumerate(data):
            MFM = (getattr(entry, close) - data[i].low) - (data[i].high - getattr(entry, close)) / (data[i].high - data[i].low)
            MFV = MFM * entry.volume

            if len(ad) == 0:
                ad.append(MFV)
                prev_value = MFV
            else:
                ad.append(prev_value + MFV)
                prev_value = ad[-1]
                ad_dict['data'].append({
                    'date' : entry.date,
                    'ad' : ad[-1]
                })
        return ad_dict


    def obv_ind(self, ticker, use_adj_close = False):
        data = stock_service().select_all_stock_objs(ticker)
        obv_dict = {
            'ticker': ticker,
            'data': []
        }
        obv = []
        for i, entry in enumerate(data):
            close = 'close'
            if use_adj_close:
                close = 'adj_close'
            if i > 1:
                volume_direction = 0
                if getattr(data[i], close) > getattr(data[i-1], close):
                    volume_direction = 1
                    # volume = data[i].volume
                elif getattr(data[i], close) < getattr(data[i-1], close):
                    volume_direction = -1
                    # volume = -data[i].volume

                if len(obv) == 0:
                    obv.append(entry.volume * volume_direction)
                else:
                    if volume_direction == 0:
                        obv.append(obv[-1])
                    else:
                        obv.append(obv[-1] + (entry.volume * volume_direction))
                obv_dict['data'].append({
                    'date' : entry.date,
                    'obv' : obv[-1]
                })
        return obv_dict