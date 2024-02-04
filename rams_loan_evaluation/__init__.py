import pandas as pd
import numpy as np
import numpy_financial as npf
import math
import sys
import sqlite3

def main():
    loan_df_final, loan_df, loan_list = gather_input_dataframes(f"test_files/{sys.argv[1]}")

    reduced_columns = [
        'loanId','servicingCostDollar','holdToMaturity','saleDateMonths','applyBuyerDiscRate','buyerDiscRate','investorDiscRate','saleExitPrice'
    ]

    prepayment_penalty = pd.DataFrame(prepayment_penalty_df(loan_list))
    default_recovery_rates = pd.DataFrame(default_recovery_df(loan_list))
    forbear_rates = pd.DataFrame(forbear_df(loan_list))
    loan_schedule = pd.DataFrame(amortization_schedule(loan_list))

    loan_level = loop_through_loans(
        loan_df[reduced_columns], prepayment_penalty, default_recovery_rates, forbear_rates, loan_schedule
    )

    reduced_loan_level_columns = [
        'loanId',
        'totalNotionalDefault',
        'defaultRecoveryAppliedNpv',
        'forbearanceRecoveryAppliedNpv',
        'monthlyForbearanceAppliedNpv',
        'prepaymentsAppliedNpv',
        'prepaymentPenaltiesAppliedNpv',
        'paymentAppliedNpv',
        'servicingCostAppliedNpv',
        'totalFvSaleExitProceeds',
        'totalFvCashflows',
        'totalLoanNpv',
        'priceBalanceRatio'
    ]

    rename_output_fields = {
        'totalNotionalDefault': 'Notional Default',
        'defaultRecoveryAppliedNpv': 'Default Recovery',
        'forbearanceRecoveryAppliedNpv': 'Forbearance Recovery',
        'monthlyForbearanceAppliedNPV': 'Monthly Forbearance',
        'prepaymentsAppliedNpv': 'Prepayments',
        'prepaymentPenaltiesAppliedNpv': 'Prepayment Penalties',
        'paymentAppliedNpv': 'Scheduled Loan Payment',
        'servicingCostAppliedNpv': 'Servicing Cost',
        'totalFvSaleExitProceeds': 'Sale/Exit Proceeds',
        'totalFvCashflows': 'Net Cashflows',
        'totalLoanNpv': 'Loan NPV',
        'priceBalanceRatio': 'Dollar Price'
    }

    final = loan_df_final.merge(
    loan_level[reduced_loan_level_columns].rename(columns=rename_output_fields), 
    left_on='LoanId', 
    right_on='loanId'
)

    final.to_excel(f"output_files/{sys.argv[2]}")

def gather_input_dataframes(file):
    loan_df_final = pd.read_excel(file)
    
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

    loan_df = loan_df.rename(columns={'LoanId': 'loanId'})
    
    loan_df.saleDateMonths = loan_df.saleDateMonths.round()

    loan_list = loan_df.values.tolist()

    return loan_df_final, loan_df, loan_list

def loop_through_loans(loans, prepayment_penalty, default_recovery, forbear_rates, loan_schedule, increment=300):
    loops = int(math.ceil(loans.count()[0] / increment))
    df_final = pd.DataFrame([])
    schedules = pd.DataFrame([])
    
    for rows in range(loops):
        lower_bound = rows * increment
        upper_bound = (rows + 1) * increment
        
        reduced_loans = loans[lower_bound:upper_bound]
        loan_list = reduced_loans['loanId'].values.tolist()
        
        schedule = cashflow_df(
                    loan_schedule[loan_schedule.loanId.isin(loan_list)], 
                    forbear_rates[forbear_rates.loanId.isin(loan_list)], 
                    default_recovery[default_recovery.loanId.isin(loan_list)], 
                    loans, 
                    prepayment_penalty[prepayment_penalty.loanId.isin(loan_list)]
                )
        
        schedule_aggregate = model_aggregation(schedule)
        
        df_final = pd.concat([df_final,schedule_aggregate])
        print(f"completed {upper_bound} loans")
        
    #ratio beginning balance to loan npv
    price_balance_ratio = round(df_final['totalLoanNpv'] / df_final['beginBalance'], 6)
    df_final['priceBalanceRatio'] = price_balance_ratio
    
    return df_final

# aggregation step
def model_aggregation(df):

    conn = sqlite3.connect(':memory:')
    conn.create_function("power", 2, sqlite_power)
    df.to_sql('cashflow_schedule', conn, index=False)
    qry = '''
        select 
            loanId,
            max(beginBalance) beginBalance,
            sum(prepayments) as totalPrepayments,
            sum(notionalDefault) as totalNotionalDefault,
            sum(principal) as totalPrincipal,
            sum(prepayments + notionalDefault + principal) as derivedBeginBalance,
            sum(interest) as totalInterest,
            sum(forbearInterest) as totalForbearInterest,
            sum(forbearPrincipal) as totalForbearPrincipal,
            sum(forbearedInterestRecovered) as totalForbearInterestRecovered,
            sum(forbearedInterestRecovered) as totalForbearPrincipalRecovered,
            sum(defaultRecovery) as totalDefaultRecovery,
            sum(forbearanceRecovery) as totalForbearanceRecovery,
            -sum(monthlyForbearance) as totalMonthlyForbearanceRecovered,
            sum(monthlyForbearance) as totalMonthlyForbearance,
            sum(prepaymentPenalties) as totalPrepaymentPenalties,
            sum(payment) as totalPayment,
            sum(servicingCost) as totalServicingCost,
            sum(netCashflows) as totalNetCashflows,
            sum(fvHoldCash) as totalFvHoldCash,
            sum(fvCashflowSale) as totalFvCashflowSale,
            sum(fvCashflowSaleDollarPrice) as totalFvCashflowSaleDollarPrice,
            sum(fvSaleExitProceeds) as totalFvSaleExitProceeds,
            sum(fvCashflows) as totalFvCashflows,
            sum(loanNpv) as totalLoanNpv,
            case when holdToMaturity = 'NO'
                then sum(case when period <= saleDateMonths then defaultRecovery else 0 end) 
                else sum(defaultRecovery)
            end as defaultRecoveryAppliedNpv,
            case when holdToMaturity = 'NO'
                then sum(case when period <= saleDateMonths then forbearanceRecovery else 0 end) 
                else sum(forbearanceRecovery)
            end as forbearanceRecoveryAppliedNpv,
            case when holdToMaturity = 'NO'
                then sum(case when period <= saleDateMonths then monthlyForbearance else 0 end) 
                else sum(monthlyForbearance)
            end as monthlyForbearanceAppliedNpv,
            case when holdToMaturity = 'NO'
                then sum(case when period <= saleDateMonths then prepayments else 0 end) 
                else sum(prepayments)
            end as prepaymentsAppliedNpv,
            case when holdToMaturity = 'NO'
                then sum(case when period <= saleDateMonths then prepaymentPenalties else 0 end) 
                else sum(prepaymentPenalties)
            end as prepaymentPenaltiesAppliedNpv,
            case when holdToMaturity = 'NO'
                then sum(case when period <= saleDateMonths then payment else 0 end) 
                else sum(payment)
            end as paymentAppliedNpv,
            case when holdToMaturity = 'NO'
                then sum(case when period <= saleDateMonths then servicingCost else 0 end)
                else sum(servicingCost)
            end as servicingCostAppliedNpv,
            case when holdToMaturity = 'NO'
                then sum(case when period <= saleDateMonths then netCashflows else 0 end)
                else sum(netCashflows)
            end as netCashflowsAppliedNpv

        from cashflow_schedule
        group by loanId
        '''

    loan_level_aggreates = pd.read_sql_query(qry, conn)
    return loan_level_aggreates

def cashflow_df(loan_schedule, forbear_rates, default_recovery_rates, loan_df, prepayment_penalty):

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
        
        forbearance_recovery as (
        select 
            ci.loanId, 
            24 as recoveryPeriod, 
            -sum(ci.forbearInterest) as forbearedInterestRecovered,
            -sum(ci.forbearPrincipal) as forbearedPrincipalRecovered,
            -sum(ci.monthlyForbearance) as forbearanceRecovery 
            from cashflow_inputs ci 
            where ci.period between 1 and 24
            group by ci.loanId, recoveryPeriod
            
        ),

        add_net_cashflow as (
        select 
            ci.*,
            coalesce(fr.forbearedInterestRecovered,0) as forbearedInterestRecovered,
            coalesce(fr.forbearedPrincipalRecovered,0) as forbearedPrincipalRecovered,
            coalesce(fr.forbearanceRecovery,0) as forbearanceRecovery,
            (monthlyForbearance + prepayments + payment + servicingCost + defaultRecovery + prepaymentPenalties + coalesce(fr.forbearanceRecovery,0)) as netCashflows   
        from cashflow_inputs ci
        left outer join forbearance_recovery fr 
        on ci.loanId = fr.loanId
        and ci.period = fr.recoveryPeriod
        ),

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
                (ac.period = saleDateMonths and applyBuyerDiscRate = 'NO')
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
    return cashflow_schedule_df

def sqlite_power(x,n):
    return float(x)**n

def prepayment_penalty_df(file):
    rates = [47, 48, 49, 50, 51]
    
    for row in file:
        num_periods = int(row[18])
        for period in range(6):
            if period == 5:
                tierHigh = num_periods
                rate = float(0)
            else:
                tierHigh = int((period+1)*12)
                rate = float(row[rates[period]])
                             
            yield dict(
                [
                    ('loanId', row[2]),
                    ('tierLow', int((period*12)+1)),
                    ('tierHigh', tierHigh),
                    ('rate', rate)
                ]
            )

# Only two options instead of 3 in the dataset because the 3rd is for the month 360, this could be flawed.
def default_recovery_df(file):
    for row in file:
        num_periods = int(row[18])

        yield dict(
            [
                ('loanId', row[2]),
                ('tierLow', 1),
                ('tierHigh', 35),
                ('rate', float(row[43]))
            ]
        )
        
        yield dict(
            [
                ('loanId', row[2]),
                ('tierLow', 36),
                ('tierHigh', num_periods),
                ('rate', float(row[44]))
            ]
        )

def forbear_df(file):
    
    tier_location = [19, 21, 23, 25, 27]
    rates = [20, 22, 24, 26, 28]

    for row in file:
        for tier in range(len(tier_location)):
            if tier == 0:
                tier_low = 1
            else:
                tier_low = int(row[tier_location[tier-1]]) + 1

            yield dict(
                [
                    ('loanId', row[2]),
                    ('tierLow', tier_low),
                    ('tierHigh', int(row[tier_location[tier]])),
                    ('rate', float(row[rates[tier]]))
                ]

            )
def amortization_schedule(file):

    for row in file:
        num_periods = int(row[18])
        loan_id = row[2]
        interest_rate = float(row[17])
        beg_balance = int(row[16])
        cdr = float(row[42])
        annual_payments = 12
        
        cpr_tiers = [int(row[32]), int(row[34]), int(row[36]), int(row[38]), int(row[40])]
        cpr_rates = [float(row[33]), float(row[35]), float(row[37]), float(row[39]), float(row[41])]
        
        default_recovery_beg_month = int(row[52])
        
        
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
            
            # Calculate the Principal
            principal = round(pmt - interest, 0)
            
            # Calculate the prepayment smm and prepayment
            prepayment_smm = round(1-(1-rate_tier(cpr_tiers, cpr_rates, p))**(1/annual_payments),6)
            prepayments = round(prepayment_smm * (beg_balance - principal), 0)
            
            # Ensure prepayment gets adjusted if the loan is being paid off
            #prepayments = round(min(prepayments, beg_balance - principal), 0)

            # Determine monthly payment based on whether or not this period will pay off the loan
            pmt = round(min(pmt, beg_balance + interest), 0)

            # Calculate the notional default
            notional_default = round(notional_default_rate(p, default_recovery_beg_month, balance_list) * monthly_cdr, 0)
            
            #End payment schedule when beg balance < 100
            
            #need to add one more thing. If the sum of principal, prepayment, and notional default
            #is greater than the remaining balance, then give everything to the principal
            if beg_balance <= 100 or (principal + prepayments + notional_default) > beg_balance:
                pmt = beg_balance
                principal = beg_balance
                interest = 0
                notional_default = 0
                prepayments = 0
            
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

# This determines the rate within a range of time
def rate_tier(tiers, rates, period):
    numpy_tier_array = np.array(tiers)
    tier_index = np.argwhere(numpy_tier_array >= period)
    
    # If we are outside the bounds, the rate is 0
    if tier_index.size == 0:
        selected_rate = 0 
    else:
        selected_rate = rates[min(tier_index)[0]]
    
    return round(selected_rate,6)


if __name__ == "__main__":
    main()