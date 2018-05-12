import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from logbook import Logger, FileHandler
import datetime

from catalyst import run_algorithm
from catalyst.api import (record, symbol, order,)
from catalyst.exchange.utils.stats_utils import extract_transactions
from catalyst.exchange.utils.exchange_utils import get_exchange_symbols


NAMESPACE = 'OLMAR'
log = Logger(NAMESPACE)
file_handler = FileHandler("olmar.log")
# file_handler.push_application()

def initialize(context):
    context.ASSET_NAMES = ['btc_usdt', 'eth_usdt']
    # context.exchange = list(context.exchanges.values())[0].name.lower()
    # context.base_currency = list(context.exchanges.values())[0].base_currency.lower()
    # import pdb; pdb.set_trace()
    context.assets = [symbol(asset_name) for asset_name in context.ASSET_NAMES]
    context.m = len(context.assets) + 1 # +1 for cash
    context.b_t = np.ones(context.m) / context.m
    context.eps = 2  #change epsilon here
    context.init = False
    context.counter = 0
    context.window = 5 # in minutes
    
    #set_slippage(slippage.VolumeShareSlippage(volume_limit=0.25, price_impact=0, delay=datetime.timedelta(minutes=0)))
    #set_commission(commission.PerShare(cost=0))
    
def handle_data(context, data):    
    # import pdb; pdb.set_trace()
    for i, asset in enumerate(context.assets):
        price = data.current(asset, 'price')
        eval("record(asset_{0} = price)".format(i))

    context.counter += 1
    if context.counter <= context.window:
        return
    if not context.init:
        rebalance_portfolio(context, data, context.b_t)
        context.init = True
        return

    m = context.m

    x_tilde = np.zeros(m)
    x_tilde[0] = 1 # cash

    b = np.zeros(m)

    # find relative moving average price for each security
    for i, asset in enumerate(context.assets):
        price = data.current(asset, 'price')

        # Compute moving averages calling data.history() for each
        # moving average with the appropriate parameters. We choose to use
        # minute bars for this simulation -> freq="1m"
        # Returns a pandas dataframe.
        mavg = data.history(asset,
                                  'price',
                                  bar_count=context.window,
                                  frequency="1T",
                                  ).mean()

        x_tilde[i+1] = mavg / price
    print("x_tilde: {0}".format(x_tilde))

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
    variability = (np.linalg.norm(mark_rel_dev))**2 ##TODO: Understand WHY DELETE ITT HERE
    log.debug("Variability: {norm}".format(norm=variability))
    # test for divide-by-zero case
    if variability == 0.0:
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
    print("B: {0}".format(b))
    b_norm = simplex_projection(b)
    np.testing.assert_almost_equal(b_norm.sum(), 1)
    
    rebalance_portfolio(context, data, b_norm)
        
    # Predicted return with new portfolio
    pred_return = np.dot(b_norm, x_tilde)
    log.debug("Predicted return: {pred_return}".format(pred_return=pred_return))
    
    # Make sure that we actually optimized our objective
    assert exp_return-.001 <= pred_return, "{new} <= {old}".format(new=exp_return, old=pred_return)
    # update portfolio
    context.b_t = b_norm
    
def rebalance_portfolio(context, data, desired_port):
    log.debug('desired {0}'.format(desired_port))
    desired_amount = np.zeros_like(desired_port)
    current_amount = np.zeros_like(desired_port)
    prices = np.zeros_like(desired_port)
        
    if context.init:
        positions_value = context.portfolio.starting_cash
    else:
        positions_value = context.portfolio.positions_value + context.portfolio.cash
        
    current_amount[0] = context.portfolio.cash
    prices[0] = 1
    for i, asset in enumerate(context.assets):
        current_amount[i+1] = context.portfolio.positions[asset].amount
        prices[i+1] = data.current(asset, 'price')
    desired_amount = np.round(desired_port * positions_value / prices)
    diff_amount = desired_amount - current_amount
    for i, asset in enumerate(context.assets):
        order(asset, diff_amount[i+1]) #order_stock
    # import pdb; pdb.set_trace()

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
    # Get the base_currency that was passed as a parameter to the simulation
    exchange = list(context.exchanges.values())[0]
    base_currency = exchange.base_currency.upper()
    # import pdb; pdb.set_trace()
    # for i, asset in enumerate(context.assets):
    #     exec("asset_prices=perf.asset_{0}".format(i))
    print(perf.algorithm_period_return)
    print("Stupid Algorithm Return(BTC): {0}%".format(100*perf.asset_0[-1]/perf.asset_0[0]))
    print("Stupid Algorithm Return(ETH): {0}%".format(100*perf.asset_1[-1]/perf.asset_1[0]))
    print("OLMAR Return: {0}%".format(100+100*perf.algorithm_period_return[-1]))

    ax1 = plt.subplot(311)
    perf.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('portfolio value')
    for i, asset in enumerate(context.assets):
        ax2 = plt.subplot(312+i, sharex=ax1)
        eval("perf.asset_"+str(i)+".plot(ax=ax2)")
        ax2.set_ylabel('{0} price'.format(asset.symbol.split("_")[0]))
    plt.show()


    # Second chart: Plot asset price, moving averages and buys/sells
    # ax2 = plt.subplot(412, sharex=ax1)
    # perf.loc[:, ['price', 'short_mavg', 'long_mavg']].plot(
    #     ax=ax2,
    #     label='Price')
    # ax2.legend_.remove()
    # ax2.set_ylabel('{asset}\n({base})'.format(
    #     asset=context.asset.symbol,
    #     base=base_currency
    # ))
    # start, end = ax2.get_ylim()
    # ax2.yaxis.set_ticks(np.arange(start, end, (end - start) / 5))

    # transaction_df = extract_transactions(perf)
    # if not transaction_df.empty:
    #     buy_df = transaction_df[transaction_df['amount'] > 0]
    #     sell_df = transaction_df[transaction_df['amount'] < 0]
    #     ax2.scatter(
    #         buy_df.index.to_pydatetime(),
    #         perf.loc[buy_df.index, 'price'],
    #         marker='^',
    #         s=100,
    #         c='green',
    #         label=''
    #     )
    #     ax2.scatter(
    #         sell_df.index.to_pydatetime(),
    #         perf.loc[sell_df.index, 'price'],
    #         marker='v',
    #         s=100,
    #         c='red',
    #         label=''
    #     )

    # # Third chart: Compare percentage change between our portfolio
    # # and the price of the asset
    # ax3 = plt.subplot(413, sharex=ax1)
    # perf.loc[:, ['algorithm_period_return', 'price_change']].plot(ax=ax3)
    # ax3.legend_.remove()
    # ax3.set_ylabel('Percent Change')
    # start, end = ax3.get_ylim()
    # ax3.yaxis.set_ticks(np.arange(start, end, (end - start) / 5))

    # # Fourth chart: Plot our cash
    # ax4 = plt.subplot(414, sharex=ax1)
    # perf.cash.plot(ax=ax4)
    # ax4.set_ylabel('Cash\n({})'.format(base_currency))
    # start, end = ax4.get_ylim()
    # ax4.yaxis.set_ticks(np.arange(0, end, end / 5))

    plt.show()


if __name__ == '__main__':
    
    run_algorithm(
            capital_base=100000,
            data_frequency='daily',
            initialize=initialize,
            handle_data=handle_data,
            analyze=analyze,
            exchange_name='poloniex',
            algo_namespace=NAMESPACE,
            base_currency='usdt',
            start=pd.to_datetime('2017-01-20', utc=True),
            end=pd.to_datetime('2018-04-23', utc=True),
            output="OLMAR.out"
        )
