import unittest
import pandas as pd
import numpy as np
import numpy_financial as npf
from rams_loan_evaluation import *

class TestEvaluationModel(unittest.TestCase):

    def setUp(self):
        self.loan_df_final, self.loan_df, self.loan_list = gather_input_dataframes(f"unit_test_files/test_excel_file_input_300_loans.xlsm")
        reduced_columns = [
            'loanId','servicingCostDollar','holdToMaturity','saleDateMonths','applyBuyerDiscRate','buyerDiscRate','investorDiscRate','saleExitPrice'
        ]

        self.prepayment_penalty = pd.DataFrame(prepayment_penalty_df(self.loan_list))
        self.default_recovery_rates = pd.DataFrame(default_recovery_df(self.loan_list))
        self.forbear_rates = pd.DataFrame(forbear_df(self.loan_list))
        self.loan_schedule = pd.DataFrame(amortization_schedule(self.loan_list))
        self.cashflow_schedule = cashflow_df(
            self.loan_schedule, 
            self.forbear_rates, 
            self.default_recovery_rates, 
            self.loan_df[reduced_columns], 
            self.prepayment_penalty
        )

    def test_amortization_schedule(self):

        comparison_df = self.loan_schedule
        base_df = pd.read_excel(f"unit_test_files/amortization_schedule_unit_test.xlsx")
        test_df = pd.DataFrame(index=base_df.index)

        compare_metrics = [
            'beginBalance',
            'endBalance',
            'interest',
            'notionalDefault',
            'payment',
            'prepayments',
            'principal'
        ]

        for metric in range(len(compare_metrics)):
        
            testValue = base_df[[compare_metrics[metric]]]
            newValue = comparison_df[[compare_metrics[metric]]]

            test_df[compare_metrics[metric]] = abs(round((testValue - newValue),0))
            
            self.assertTrue(test_df[test_df[[compare_metrics[metric]]] > 0][compare_metrics[metric]].count() == 0)
    
    
    def test_cashflow_schedule(self):

        comparison_df = self.cashflow_schedule
        base_df = pd.read_excel(f"unit_test_files/cashflow_schedule_unit_test.xlsx")
        test_df = pd.DataFrame(index=base_df.index)

        compare_metrics = [
            'beginBalance',
            'loanNpv',
            'prepayments',
            'netCashflows'
        ]

        for metric in range(len(compare_metrics)):
        
            testValue = base_df[[compare_metrics[metric]]]
            newValue = comparison_df[[compare_metrics[metric]]]

            test_df[compare_metrics[metric]] = abs(round((testValue - newValue),0))
            
            self.assertTrue(test_df[test_df[[compare_metrics[metric]]] > 0][compare_metrics[metric]].count() == 0)

    def test_prepayment_penalty(self):
        comparison_df = self.prepayment_penalty
        base_df = pd.read_excel(f"unit_test_files/prepayment_unit_test.xlsx")
        test_df = pd.DataFrame(index=base_df.index)

        testValue = base_df[['rate']]
        newValue = comparison_df[['rate']]

        test_df['rate'] = abs(round((testValue - newValue),0))

        self.assertTrue(test_df[test_df[['rate']] > 0]['rate'].count() == 0)
    
    def test_default_recovery(self):
        comparison_df = self.default_recovery_rates
        base_df = pd.read_excel(f"unit_test_files/default_recovery_unit_test.xlsx")
        test_df = pd.DataFrame(index=base_df.index)

        testValue = base_df[['rate']]
        newValue = comparison_df[['rate']]

        test_df['rate'] = abs(round((testValue - newValue),0))

        self.assertTrue(test_df[test_df[['rate']] > 0]['rate'].count() == 0)
    
    def test_forbearance(self):
        comparison_df = self.forbear_rates
        base_df = pd.read_excel(f"unit_test_files/forbear_rates_unit_test.xlsx")
        test_df = pd.DataFrame(index=base_df.index)

        testValue = base_df[['rate']]
        newValue = comparison_df[['rate']]

        test_df['rate'] = abs(round((testValue - newValue),0))

        self.assertTrue(test_df[test_df[['rate']] > 0]['rate'].count() == 0)

if __name__ == '__main__':
    unittest.main()