import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from logbook import Logger, FileHandler
import datetime

from catalyst import run_algorithm
from catalyst.api import (record, symbol, order, order_target_percent)
from catalyst.exchange.utils.stats_utils import extract_transactions
from catalyst.exchange.utils.exchange_utils import get_exchange_symbols


NAMESPACE = 'OLMAR'
log = Logger(NAMESPACE)
file_handler = FileHandler("olmar.log")
# file_handler.push_application()

def initialize(context):
    context.ASSET_NAMES = ['btc_usdt']#, 'eth_usdt', 'xrp_usdt']

    # import pdb; pdb.set_trace()
    context.assets = [symbol(asset_name) for asset_name in context.ASSET_NAMES]
    context.m = len(context.assets) + 1 # +1 for cash
    context.b_t = np.ones(context.m) / context.m
    context.eps = 1.4  #change epsilon here
    context.init = True
    context.counter = 0
    context.window = 4 # in minutes
    
    #set_slippage(slippage.VolumeShareSlippage(volume_limit=0.25, price_impact=0, delay=datetime.timedelta(minutes=0)))
    #set_commission(commission.PerShare(cost=0))
    
def handle_data(context, data):    
    # import pdb; pdb.set_trace()

    m = context.m

    x_tilde = np.zeros(m)
    mavg = np.zeros(m)
    prices = np.zeros(m)
    x_tilde[0] = 1 # cash
    mavg[0] = 1 # cash
    prices[0] = 1 # cash

    # find relative moving average price for each security
    for i, asset in enumerate(context.assets):
        prices[i+1] = data.current(asset, 'price')
        exec("record({0}_price=prices[i+1])".format(asset.symbol.split("_")[0]))
        # Compute moving averages calling data.history() for each
        # moving average with the appropriate parameters. We choose to use
        # minute bars for this simulation -> freq="1m"
        # Returns a pandas dataframe.
        mavg[i+1] = data.history(asset,
                                  'price',
                                  bar_count=context.window,
                                  frequency="1d", # CHANGE TO MINUTE IF MINUTES
                                  ).mean()

        x_tilde[i+1] = mavg[i+1] / prices[i+1]

    record(prices=prices)

    context.counter += 1
    if context.counter <= context.window:
        return
    if context.init:
        rebalance_portfolio(context, data, context.b_t)
        context.init = False
        return

    b = np.zeros(m)

    log.debug("x_tilde: {0}".format(x_tilde))

    ###########################
    # Inside of OLMAR (algo 2)
    x_bar = x_tilde.mean()
        
    # market relative deviation
    mark_rel_dev = x_tilde - x_bar

    # Expected return with current portfolio
    exp_return = np.dot(context.b_t, x_tilde)
    log.debug("Expected Return: {exp_return}".format(exp_return=exp_return))
    weight = context.eps - exp_return
    log.debug("Weight: {weight}".format(weight=weight))
    variability = (np.linalg.norm(mark_rel_dev))**2
    log.debug("Variability: {norm}".format(norm=variability))
    # test for divide-by-zero case
    if variability <= np.finfo(np.float64).eps:
        step_size = 0 # no portolio update
    else:
        step_size = max(0, weight/variability)
        # step_size = 100
    log.debug("Step-size: {size}".format(size=step_size))
    log.debug("Market relative deviation:")
    log.debug(mark_rel_dev)
    log.debug("Weighted market relative deviation:")
    log.debug(step_size*mark_rel_dev)
    b = context.b_t + step_size*mark_rel_dev
    log.debug("B: {0}".format(b))
    b_norm = simplex_projection(b)
    np.testing.assert_almost_equal(b_norm.sum(), 1)
    
    rebalance_portfolio(context, data, b_norm)
        
    # Predicted return with new portfolio
    pred_return = np.dot(b_norm, x_tilde)
    log.debug("Predicted return: {pred_return}".format(pred_return=pred_return))
    
    # update portfolio
    context.b_t = b_norm

    record(btc_holdings = b_norm[1])
    record(btc_mavg = mavg[1])

    # Make sure that we actually optimized our objective
    assert exp_return-.001 <= pred_return, "{new} <= {old}".format(new=exp_return, old=pred_return)
    
def rebalance_portfolio(context, data, desired_port):
    for i, asset in enumerate(context.assets):
        order_target_percent(asset, desired_port[i+1]) #order_stock


def simplex_projection(v, b=1):
    """Projection vectors to the simplex domain
    Implemented according to the paper: Efficient projections onto the
    l1-ball for learning in high dimensions, John Duchi, et al. ICML 2008.
    Implementation Time: 2011 June 17 by Bin@libin AT pmail.ntu.edu.sg
    Optimization Problem: min_{w}\| w - v \|_{2}^{2}
    s.t. sum_{i=1}^{m}=z, w_{i}\geq 0
    Input: A vector v \in R^{m}, and a scalar z > 0 (default=1)
    Output: Projection vector w
    :Example:
    >>> proj = simplex_projection([.4 ,.3, -.4, .5])
    >>> print proj
    array([ 0.33333333, 0.23333333, 0. , 0.43333333])
    >>> print proj.sum()
    1.0
    Original matlab implementation: John Duchi (jduchi@cs.berkeley.edu)
    Python-port: Copyright 2012 by Thomas Wiecki (thomas.wiecki@gmail.com).
    """

    v = np.asarray(v)
    p = len(v)

    # Sort v into u in descending order
    v = (v > 0) * v
    u = np.sort(v)[::-1]
    sv = np.cumsum(u)

    rho = np.where(u > (sv - b) / np.arange(1, p+1))[0][-1]
    theta = np.max([0, (sv[rho] - b) / (rho+1)])
    w = (v - theta)
    w[w<0] = 0
    return w

def analyze(context, perf):

    # import pdb; pdb.set_trace()
    for i, asset in enumerate(context.assets):
        print("B&H ({0}) Return: {1}%".format(asset.symbol.split("_")[0], -100+100*perf.prices.values[-1][i+1]/perf.prices.values[context.window][i+1]))
    print("OLMAR Return: {0}%".format(100*perf.algorithm_period_return[-1]))

    ax1 = plt.subplot(313)
    portfolio_value_norm = perf.portfolio_value / perf.portfolio_value[0]
    portfolio_value_norm.plot(ax=ax1)
    ax1.set_ylabel('portfolio value')
    ax2 = plt.subplot(311, sharex=ax1)
    ax2.set_ylabel('Asset Prices (Norm)')
    for i, asset in enumerate(context.assets):
        # exec("perf.{0}_price = perf.{0}_price / perf.{0}_price[0]".format(asset.symbol.split("_")[0]))
        exec("perf.{0}_price.plot(ax=ax2)".format(asset.symbol.split("_")[0]))
    perf.btc_mavg.plot(ax=ax2)
    plt.legend()
    ax3 = plt.subplot(312, sharex=ax1)
    perf.btc_holdings.plot(ax=ax3)
    ax3.set_ylabel('BTC Hold Percent')
    plt.show()

if __name__ == '__main__':
    
    run_algorithm(
            capital_base=1000000,
            data_frequency='daily',
            initialize=initialize,
            handle_data=handle_data,
            analyze=analyze,
            exchange_name='poloniex',
            algo_namespace=NAMESPACE,
            base_currency='usdt',
            start=pd.to_datetime('2017-01-01', utc=True),
            end=pd.to_datetime('2018-01-01', utc=True),
            output="OLMAR.out"
        )
