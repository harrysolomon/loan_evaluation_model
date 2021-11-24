import pandas as pd
from datetime import date
import numpy as np
import numpy_financial as npf
from collections import OrderedDict
import sys
import csv 

def main():
    loan_schedules = pd.DataFrame(amortize(sys.argv[1]))
    loan_schedules = horizontal_schedule_calcs(loan_schedules)

    col_order = [
        'Begin Balance',
        'Prepayments',
        'Notional Default',
        'Principal',
        'End Balance',
        'Interest',
        'Forbeared Monthly Interest',
        'Forbeared Monthly Principal',
        'Forbeared Interest Recover',
        'Forbeared Principal Recover',
        'Default Recover',
        'Forbearance Recovery',
        'Monthly Forbearance',
        'Payment',
        'Servicing Cost',
        'Net Cashflow',
        'FV Holding Period Cash',
        'FWD Cash Sale',
        'FV Sale Dollar Price',
        'Sale Exit Proceeds',
        'FV Cashflows',
        'Loan NPV'
    ]

    aggregation_config = {
        'Begin Balance': 'max',
        'Prepayments': 'sum',
        'Notional Default': 'sum',
        'Principal': 'sum',
        'End Balance': 'min',
        'Interest': 'sum',
        'Forbeared Monthly Interest': 'sum',
        'Forbeared Monthly Principal': 'sum',
        'Forbeared Interest Recover': 'sum',
        'Forbeared Principal Recover': 'sum',
        'Default Recover': 'sum',
        'Forbearance Recovery': 'sum',
        'Monthly Forbearance': 'sum',
        'Payment': 'sum',
        'Servicing Cost': 'sum',
        'Net Cashflow': 'sum',
        'FV Holding Period Cash': 'sum',
        'FWD Cash Sale': 'sum',
        'FV Sale Dollar Price': 'sum',
        'Loan NPV': 'sum',
        'Sale Date Months': 'max',
        'Investor Discount Rate': 'max'
    }

    dimension_config = [
        'Loan ID'
    ]

    loan_schedule_aggregates = loan_schedules.groupby(dimension_config).aggregate(aggregation_config)
    final_df = post_aggregation_calcs(loan_schedule_aggregates)
    output = final_df[col_order]

    output.to_csv(sys.argv[2])

def notional_default_rate(period, month, balance_list):
    if period >= month:
        base_balance = balance_list[period-month]
    else:
        base_balance = 0 
    
    return base_balance

# This determines the rate within a range of time
def rate_tier(tiers, rates, period):
    numpy_tier_array = np.array(tiers)
    tier_index = np.argwhere(numpy_tier_array <= period)
    selected_rate = rates[max(tier_index)[0]]
    return round(selected_rate,6)

# This is used instead of the rate_tier function when a rate is only needed for a specific period rather than a range
def rate_specific_period(tiers, rates, period):
    if period in tiers:
        rate_idx = tiers.index(period)
        rate = rates[rate_idx]
    else:
        rate = 0 
    return rate

# Determine the periods in which the current owner is accruing loan payments (prior to sale)
def fv_hold_period_cash(sale_date, p):
    if p <= sale_date:
        fv_cash = 1
    else:
        fv_cash = 0
        
    return fv_cash

# determine the present value (using the buyers discount rate) for a future loan sale
def cashflow_of_sale(hold_maturity, use_discount, period, sale_date, buy_disc_rate):
    if hold_maturity == 'NO' and use_discount == 'YES' and period > sale_date:
        buy_pv_factor = round((1/((1+(buy_disc_rate/12))**(period-sale_date))),6)
    else:
        buy_pv_factor = 0 
    return buy_pv_factor

# Determine if we are applying the remaining balance to the sale instead of buyer discount rate of remaining cash flows
def fv_dollar_price(use_discount, period, sale_date, balance, exit_price_pct):
    if use_discount == 'NO' and period == sale_date:
        fv_price = round(balance * exit_price_pct,0)
    else:
        fv_price = 0 
    return fv_price

# determine if a servicing cost should be incurred
def service_cost_amt(balance, service_cost):
    if balance > 0:
        final_cost = service_cost
    else:
        final_cost = 0 
    return final_cost

# Any calculations that can be calculated horizontally using already gathered information
# pmt and prepayment are exempt because we need to apply min calculation to them in order to change 
# their values in case the original payment or prepayment is more than we need

def horizontal_schedule_calcs(schedule_df):
    
    # Calculate Forbearance
    forbearance_interest = round(schedule_df['Interest'] * schedule_df['Monthly Forbearance Rate'],0)
    forbearance_principal = round(schedule_df['Principal'] * schedule_df['Monthly Forbearance Rate'],0)
    
    schedule_df['Forbeared Monthly Interest'] = forbearance_interest
    schedule_df['Forbeared Monthly Principal'] = forbearance_principal
    
    # Calculate Forbearance Recovery
    forbearance_recovery = round(-schedule_df['Forbeared Interest Recover'] - schedule_df['Forbeared Principal Recover'],0)
    schedule_df['Forbearance Recovery'] = forbearance_recovery
    
    # Calculate Monthly Forbearance
    monthly_forbearance = round(-schedule_df['Payment'] * schedule_df['Monthly Forbearance Rate'],0)
    schedule_df['Monthly Forbearance'] = monthly_forbearance
    
    # Calculate Net Cashflows
    net_cashflow = round(
        schedule_df['Default Recover'] +
        schedule_df['Forbearance Recovery'] +
        schedule_df['Monthly Forbearance'] + 
        schedule_df['Prepayments'] + 
        schedule_df['Payment'] + 
        schedule_df['Servicing Cost']
        , 0
    )
    
    schedule_df['Net Cashflow'] = net_cashflow
    
    fv_holding_period_cash = round(schedule_df['Net Cashflow'] * schedule_df['FV Holding Period Cash'],0)
    fwd_cash_sale = round(schedule_df['Net Cashflow'] * schedule_df['FWD Cash Sale'],0)
    
    schedule_df['FV Holding Period Cash'] = fv_holding_period_cash
    schedule_df['FWD Cash Sale'] = fwd_cash_sale
    
    # Calculate PV Factor
    pv_factor = round((1/(1+(schedule_df['Investor Discount Rate']/12))**schedule_df['Period']),6)
    schedule_df['PV Factor'] = pv_factor 
    
    # Calculate Loan NPV
    loan_npv = round(schedule_df['PV Factor'] * schedule_df['FV Holding Period Cash'], 0)
    schedule_df['Loan NPV'] = loan_npv
    
    return schedule_df

def post_aggregation_calcs(df):
    pv_factor = round((1/(1+(df['Investor Discount Rate']/12))**df['Sale Date Months']),6)
    sale_exit_proceeds = round(df['FWD Cash Sale'] + df['FV Sale Dollar Price'], 0)
    pv_sale_exit_proceeds = round(pv_factor * sale_exit_proceeds, 0)
    adj_loan_npv = round(df['Loan NPV'] + pv_sale_exit_proceeds, 0)
    fv_cashflow = round(sale_exit_proceeds + df['FV Holding Period Cash'],0)
    
    df['Sale Exit Proceeds'] = sale_exit_proceeds
    df['Loan NPV'] = adj_loan_npv
    df['FV Cashflows'] = fv_cashflow
    
    return df

def amortize(file, annual_payments=12):
    
    loan_csv = open(file)
    loan_csvreader = csv.reader(loan_csv)
    next(loan_csvreader)
    
    for row in loan_csvreader:
        interest_rate = float(row[2])
        num_periods = int(row[3])
        discount_rate = float(row[40])
        beg_balance = int(row[1])
        cpr_tiers = [int(row[21]), int(row[23]), int(row[25]), int(row[27]), int(row[29])]
        cpr_rates = [float(row[22]), float(row[24]), float(row[26]), float(row[28]), float(row[30])]
        forbear_tier = [int(row[5]), int(row[7]), int(row[9]), int(row[11]), int(row[13])]
        forbear_rates = [float(row[6]), float(row[8]), float(row[10]), float(row[12]), float(row[14])]
        forbear_recovery_tier = [int(row[15]), int(row[17]), int(row[19])]
        forbear_recovery_rate = [float(row[16]), float(row[18]), float(row[20])]
        default_recovery_tier = [int(row[32]), int(row[34]), int(row[36]), int(row[36])+1]
        default_recovery_rate = [float(row[33]), float(row[35]), float(row[37]), float(row[39])]
        default_recovery_beg_month = int(row[38])
        cdr = float(row[31])
        remaining_life_pool = float(row[39])
        sale_apply_disc_rate = str(row[41])
        buyer_disc_rate = float(row[42])
        sale_or_exit_price = float(row[43])
        servicing_cost = float(row[44])
        servicing_cost_post = float(row[45])
        hold_to_maturity = str(row[46])
        sale_date_months = int(row[47])
    
        # initialize the variables to keep track of the periods and running balances
        p = 1
        
        end_balance = beg_balance
        
        # need this for notional default and default recovery
        balance_list = list([])
        forbearance_interest_list = list([])
        forbearance_principal_list = list([])
        monthly_cdr = round(1-(1-cdr)**(1/12),6)
        

        while end_balance > 0:
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
            
            # Calculate default recovery
            default_recovery = round(notional_default * rate_tier(default_recovery_tier, default_recovery_rate, p),0)
            
            # End Balance
            end_balance = round(beg_balance - (principal + prepayments + notional_default), 0)
            
            #Calculate cumulative forbearance, do this before the current period forbearance
            #Because forbearance recovery must use sum of forbearance up to the current period
            cum_forbearance_monthly_interest = sum(forbearance_interest_list)
            cum_forbearance_monthly_principal = sum(forbearance_principal_list)
            
            # Calculate Forbearance
            monthly_forbearance_rate = rate_tier(forbear_tier, forbear_rates, p)
            forbeared_monthly_interest = interest * monthly_forbearance_rate 
            forbeared_monthly_principal = principal * monthly_forbearance_rate
            
            forbearance_interest_list.append(forbeared_monthly_interest)
            forbearance_principal_list.append(forbeared_monthly_principal)
            
            # Calculate Forbearance Recovery
            forbearance_recovery_rate = rate_specific_period(forbear_recovery_tier, forbear_recovery_rate, p)
            forbear_interest_recovered = round(-cum_forbearance_monthly_interest * forbearance_recovery_rate,0)
            forbear_principal_recovered = round(-cum_forbearance_monthly_principal * forbearance_recovery_rate,0)
            forbearance_recovery = round((forbear_interest_recovered + forbear_principal_recovered) * -1,0)
            
            # Loan sale support
            fv_holding_period_cash = fv_hold_period_cash(sale_date_months, p)
            
            fwd_cash_sale = cashflow_of_sale(
                hold_to_maturity, sale_apply_disc_rate, p, sale_date_months, buyer_disc_rate
            )
            
            fv_sale_dollar_price = fv_dollar_price(
                sale_apply_disc_rate, p, sale_date_months, end_balance, sale_or_exit_price
            )
            
            

            yield dict(
                [
                    ('Period', p),
                    ('Loan ID',row[0]),
                    ('Investor Discount Rate', discount_rate),
                    ('Begin Balance', beg_balance),
                    ('Principal', principal),
                    ('Payment', pmt),
                    ('Interest', interest),
                    ('Notional Default', notional_default),
                    ('Default Recover', default_recovery),
                    ('Monthly Forbearance Rate', monthly_forbearance_rate),
                    ('Prepayment SMM', prepayment_smm),
                    ('Monthly CDR', monthly_cdr),
                    ('Prepayments', prepayments),
                    ('End Balance', end_balance),
                    ('Forbeared Interest Recover', forbear_interest_recovered),
                    ('Forbeared Principal Recover', forbear_principal_recovered),
                    ('Servicing Cost', -service_cost_amt(end_balance, servicing_cost)),
                    ('FV Holding Period Cash', fv_holding_period_cash),
                    ('FWD Cash Sale', fwd_cash_sale),
                    ('FV Sale Dollar Price', fv_sale_dollar_price),
                    ('Sale Date Months', sale_date_months)
                ]
            )

            # Increment the counter, balance and date
            p += 1
            beg_balance = end_balance

if __name__ == "__main__":
    main()


