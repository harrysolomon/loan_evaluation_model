import pandas as pd
from datetime import date
import numpy as np
import numpy_financial as npf
from collections import OrderedDict
import sys
import csv 
import sqlite3

def main():
    file = sys.argv[1]
    prepayment_penalty = pd.DataFrame(prepayment_penalty_df(file))
    forbearance_recovery = pd.DataFrame(forbearance_recovery_month(file))
    default_recovery_rates = pd.DataFrame(default_recovery_df(file))
    forbear_rates = pd.DataFrame(forbear_df(file))
    loan_schedule = pd.DataFrame(amortization_schedule(file))

    loan_df_final = pd.read_csv(file)

    loan_df = loan_df_final.rename(columns={
        'Loan ID': 'loanId', 
        'Servicing Cost per Month ($)': 'servicingCostDollar',
        'Hold to Maturity?': 'holdToMaturity',
        'Sale Date (months from today)': 'saleDateMonths',
        'Sale - apply Buyer Discount Rate?     [YES/NO]': 'applyBuyerDiscRate',
        'Buyer Discount Rate' : 'buyerDiscRate',
        'Investor Discount Rate' : 'investorDiscRate',
        'Sale or Exit Price': 'saleExitPrice'
    })


    conn = sqlite3.connect(':memory:')

    #Created a custom function which I am using for discount rates
    conn.create_function("power", 2, sqlite_power)

    #Convert dataframes to tables
    loan_schedule.to_sql('loan_schedule', conn, index=False)
    forbear_rates.to_sql('forbear_rates', conn, index=False)
    default_recovery_rates.to_sql('default_recovery_rates', conn, index=False)
    loan_df.to_sql('loan_df', conn, index=False)
    prepayment_penalty.to_sql('prepayment_penalty', conn, index=False)

    qry = '''
        with cashflow_inputs as (
        select  
            ls.loanId, 
            period, 
            beginBalance, 
            prepayments, 
            notionalDefault, 
            principal, 
            endBalance, 
            interest,
            payment,
            ld.holdToMaturity,
            ld.saleDateMonths,
            ld.applyBuyerDiscRate,
            ld.buyerDiscRate,
            ld.saleExitPrice,
            (interest * fr.rate) as forbearInterest, 
            (principal * fr.rate) as forbearPrincipal,
            -((interest * fr.rate) + (principal * fr.rate)) as monthlyForbearance,
            (notionalDefault * drr.rate) as defaultRecovery,
            (ls.prepayments * pp.rate) as prepaymentPenalties,
            (1/power((1+(ld.investorDiscRate/12)),ls.period)) as pv_factor,
            case when ls.endBalance > 0.5 then -ld.servicingCostDollar else 0 end as servicingCost
        from loan_schedule ls
        join forbear_rates fr 
        on ls.loanId = fr.loanId
        and ls.period between fr.tierLow and fr.tierHigh
        join loan_df ld 
        on ls.loanId = ld.loanId
        join default_recovery_rates drr 
        on drr.loanId = ls.loanId
        and ls.period between drr.tierLow and drr.tierHigh
        join prepayment_penalty pp 
        on pp.loanId = ls.loanId
        and ls.period between pp.tierLow and pp.tierHigh
        order by ls.loanId, ls.period),
        
        add_net_cashflow as (
        select 
            ci.*,
            (monthlyForbearance + prepayments + payment + servicingCost + defaultRecovery + prepaymentPenalties) as netCashflows   
        from cashflow_inputs ci),
        
        cashflow_sale as (
        select
            ac.*,
            case when 
                (ac.period <= saleDateMonths)
                then ac.netCashflows else 0 end 
            as fvHoldCash,
            case when 
                (ac.period > saleDateMonths and applyBuyerDiscRate = 'YES' and holdToMaturity = 'NO')
                then ac.netCashflows / power(1+(buyerDiscRate/12),period - saleDateMonths) else 0 end 
            as fvCashflowSale,
            case when 
                (ac.period = saleDateMonths and applyBuyerDiscRate = 'NO' and holdToMaturity = 'NO')
                then ac.endBalance * saleExitPrice else 0 end 
            as fvCashflowSaleDollarPrice
        from add_net_cashflow ac),
        
        sum_buyer_discount_sale as (
        select
            cs.loanId,
            cs.saleDateMonths,
            sum(cs.fvCashFlowSale) as fvCashflowSaleBuyDiscount
        from cashflow_sale cs
        group by loanId, saleDateMonths
        )
        
        select
            cs.*,
            (coalesce(fvCashflowSaleBuyDiscount,0) + fvCashflowSaleDollarPrice) as fvSaleExitProceeds,
            (coalesce(fvCashflowSaleBuyDiscount,0) + fvCashflowSaleDollarPrice + fvHoldCash) as fvCashflows,
            (coalesce(fvCashflowSaleBuyDiscount,0) + fvCashflowSaleDollarPrice + fvHoldCash) * pv_factor as loanNpv
        from cashflow_sale cs
        left outer join sum_buyer_discount_sale ss 
        on cs.loanId = ss.loanId
        and cs.period = ss.saleDateMonths
            
        '''
    cashflow_schedule_df = pd.read_sql_query(qry, conn)
    cashflow_schedule_df.to_csv('testing12')

def rate_tier(tiers, rates, period):
    numpy_tier_array = np.array(tiers)
    tier_index = np.argwhere(numpy_tier_array <= period)
    selected_rate = rates[max(tier_index)[0]]
    return round(selected_rate,6)

def amortization_schedule(file):
    loan_csv = open(file)
    loan_csvreader = csv.reader(loan_csv)
    next(loan_csvreader)

    for row in loan_csvreader:
        num_periods = int(row[3])
        loan_id = row[0]
        interest_rate = float(row[2])
        num_periods = int(row[3])
        discount_rate = float(row[40])
        beg_balance = int(row[1])
        cdr = float(row[31])
        annual_payments = 12
        
        cpr_tiers = [int(row[21]), int(row[23]), int(row[25]), int(row[27]), int(row[29])]
        cpr_rates = [float(row[22]), float(row[24]), float(row[26]), float(row[28]), float(row[30])]
        
        default_recovery_beg_month = int(row[38])
        
        
        # need this for notional default and default recovery
        balance_list = list([])
        p = 1
        end_balance = beg_balance
        
        while end_balance > 0:


            monthly_cdr = round(1-(1-cdr)**(1/12),6)
        
            # building the beginning balance list. Needed for notional default
            balance_list.append(beg_balance)
            
            # initial pmt calculation
            pmt = -round(npf.pmt(interest_rate/annual_payments, num_periods - (p-1), beg_balance), 0)
            
            # Recalculate the interest based on the current balance
            interest = round(((interest_rate/annual_payments) * beg_balance), 0)
            
            # Determine monthly payment based on whether or not this period will pay off the loan
            pmt = round(min(pmt, beg_balance + interest), 0)
            
            # Calculate the Principal
            principal = round(pmt - interest, 0)
            
            # Calculate the prepayment smm and prepayment
            prepayment_smm = round(1-(1-rate_tier(cpr_tiers, cpr_rates, p))**(1/annual_payments),6)
            prepayments = round(prepayment_smm * (beg_balance - principal), 0)
            
            # Ensure prepayment gets adjusted if the loan is being paid off
            prepayments = round(min(prepayments, beg_balance - principal), 0)
            
            # Calculate the notional default
            notional_default = round(notional_default_rate(p, default_recovery_beg_month, balance_list) * monthly_cdr, 0)
            
            # End Balance
            end_balance = round(beg_balance - (principal + prepayments + notional_default), 0)

            yield dict(
                    [
                        ('loanId', loan_id),
                        ('period', p),
                        ('beginBalance', beg_balance),
                        ('interest', interest),
                        ('payment', pmt),
                        ('principal', principal),
                        ('prepayments', prepayments),
                        ('notionalDefault', notional_default),
                        ('endBalance', end_balance),
                        ('interestRate', interest_rate)
                    ]
            )
            
            p += 1
            beg_balance = end_balance

def notional_default_rate(period, month, balance_list):
    if period >= month:
        base_balance = balance_list[period-month]
    else:
        base_balance = 0 
    
    return base_balance


# to do:
# Adjust the tiers to be based on the upper bound instead of lower bound
def forbear_df(file):
    
    tier_location = [5, 7, 9, 11, 13]
    rates = [6, 8, 10, 12, 14]
    
    loan_csv = open(file)
    loan_csvreader = csv.reader(loan_csv)
    next(loan_csvreader)
    for row in loan_csvreader:
        num_periods = int(row[3])
        for tier in range(len(tier_location)):
            if tier == len(tier_location) - 1:
                tier_high = num_periods
            else:
                tier_high = int(row[tier_location[tier+1]]) -1

            yield dict(
                [
                    ('loanId', row[0]),
                    ('tierLow', int(row[tier_location[tier]])),
                    ('tierHigh', tier_high),
                    ('rate', float(row[rates[tier]]))
                ]

            )

# to do:
# Adjust the tiers to be based on the upper bound instead of lower bound
def default_recovery_df(file):
    
    tier_location = [34, 36]
    rates = [35, 37]
    
    loan_csv = open(file)
    loan_csvreader = csv.reader(loan_csv)
    next(loan_csvreader)
    for row in loan_csvreader:
        num_periods = int(row[3])
        for tier in range(len(tier_location)):
            if tier == 1:
                tier_high = num_periods
                tier_low = int(row[tier_location[tier]]) + 1
            else:
                tier_high = int(row[tier_location[tier+1]])
                tier_low = int(row[tier_location[tier]])

            yield dict(
                [
                    ('loanId', row[0]),
                    ('tierLow', tier_low),
                    ('tierHigh', tier_high),
                    ('rate', float(row[rates[tier]]))
                ]

            )

def forbearance_recovery_month(file):
    
    months = [15, 17, 19]
    rates = [16, 18, 20]
    
    loan_csv = open(file)
    loan_csvreader = csv.reader(loan_csv)
    next(loan_csvreader)
    for row in loan_csvreader:
        for month in range(len(months)):

            yield dict(
                [
                    ('loanId', row[0]),
                    ('month', row[months[month]]),
                    ('rate', float(row[rates[month]]))
                ]

            )
def prepayment_penalty_df(file):
    rates = [0.02, 0.01, 0.00, 0.00, 0.00, 0.00]
    loan_csv = open(file)
    loan_csvreader = csv.reader(loan_csv)
    next(loan_csvreader)
    
    for row in loan_csvreader:
        num_periods = int(row[3])
        for period in range(6):
            if period == 5:
                tierHigh = num_periods
            else:
                tierHigh = int((period+1)*12)
            yield dict(
                    [
                        ('loanId', row[0]),
                        ('tierLow', int((period*12)+1)),
                        ('tierHigh', tierHigh),
                        ('rate', float(rates[period]))
                    ]

                )

def sqlite_power(x,n):
    return float(x)**n

if __name__ == "__main__":
    main()