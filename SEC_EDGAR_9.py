import requests
import pandas as pd
from typing import List, Dict, Optional
import time
from datetime import datetime
import json
import os
from concurrent.futures import ThreadPoolExecutor
import logging

class SECEdgarCompustatFetcher:
    """
    SEC EDGAR data fetcher for Compustat variables from 10-K reports
    """
    VARIABLE_DEFINITIONS = {    
    # Comprehensive Compustat Variable Definitions
        'CONM': {'definition': 'Company Name', 'type': 'text', 'xbrl_tag': 'EntityRegistrantName'},
        'CIK': {'definition': 'Central Index Key', 'type': 'text', 'xbrl_tag': 'CIK'},
        'FYR': {'definition': 'Fiscal Year-End Month', 'type': 'integer', 'xbrl_tag': 'CurrentFiscalYearEndDate'},
        'BUSDESC': {'definition': 'Business Description', 'type': 'text', 'xbrl_tag': 'BusinessDescription'},
        'COUNTY': {'definition': 'County', 'type': 'text', 'xbrl_tag': 'County'},
        'FYRC': {'definition': 'Fiscal Year Code', 'type': 'integer', 'xbrl_tag': 'FiscalYearCode'},
        'SIC': {'definition': 'Standard Industrial Classification', 'type': 'integer', 'xbrl_tag': 'SICCode'},
        'WEBURL': {'definition': 'Website URL', 'type': 'text', 'xbrl_tag': 'EntityWebSite'},
        'APDEDATE': {'definition': 'Date of Last Annual Report', 'type': 'date', 'xbrl_tag': 'DocumentPeriodEndDate'},
        'CURNCD': {'definition': 'ISO Currency Code', 'type': 'text', 'xbrl_tag': 'FunctionalCurrency'},
        'CURRTR': {'definition': 'Current Translation Rate', 'type': 'float', 'xbrl_tag': 'ExchangeRate'},
        'FYEAR': {'definition': 'Fiscal Year', 'type': 'integer', 'xbrl_tag': 'FiscalYear'},
        'PDDUR': {'definition': 'Period Duration', 'type': 'integer', 'xbrl_tag': 'PeriodDuration'},
    
        # Assets & Liabilities
        'ACT': {'definition': 'Current Assets', 'type': 'float', 'xbrl_tag': 'AssetsCurrent'},
        'AP': {'definition': 'Accounts Payable', 'type': 'float', 'xbrl_tag': 'AccountsPayableCurrent'},
        'APB': {'definition': 'Accounts Payable, Balance', 'type': 'float', 'xbrl_tag': 'AccountsPayable'},
        'APC': {'definition': 'Accounts Payable Changes', 'type': 'float', 'xbrl_tag': 'AccountsPayableIncreaseDecrease'},
        'APOFS': {'definition': 'Accounts Payable Offset', 'type': 'float', 'xbrl_tag': 'AccountsPayableOffset'},
        'ARB': {'definition': 'Accounts Receivable, Balance', 'type': 'float', 'xbrl_tag': 'AccountsReceivable'},
        'ARC': {'definition': 'Accounts Receivable, Changes', 'type': 'float', 'xbrl_tag': 'AccountsReceivableIncreaseDecrease'},
        'ARTFS': {'definition': 'Accounts Receivable, Trade and Other', 'type': 'float', 'xbrl_tag': 'AccountsReceivableTradeAndOtherCurrent'},
        'AT': {'definition': 'Total Assets', 'type': 'float', 'xbrl_tag': 'Assets'},
    
        # Depreciation
        'DPACB': {'definition': 'Accumulated Depreciation, Beginning', 'type': 'float', 'xbrl_tag': 'AccumulatedDepreciation'},
        'DPACC': {'definition': 'Accumulated Depreciation, Changes', 'type': 'float', 'xbrl_tag': 'DepreciationIncreaseDecrease'},
        
        # Inventory
        'INVFG': {'definition': 'Finished Goods Inventory', 'type': 'float', 'xbrl_tag': 'InventoryFinishedGoods'},
        'INVO': {'definition': 'Other Inventory', 'type': 'float', 'xbrl_tag': 'InventoryOther'},
        'INVOFS': {'definition': 'Inventory Offset', 'type': 'float', 'xbrl_tag': 'InventoryOffset'},
        'INVT': {'definition': 'Total Inventory', 'type': 'float', 'xbrl_tag': 'Inventory'},
    
        # Property, Plant & Equipment
        'PPEGT': {'definition': 'Gross PPE', 'type': 'float', 'xbrl_tag': 'PropertyPlantAndEquipmentGross'},
        'PPENT': {'definition': 'Net PPE', 'type': 'float', 'xbrl_tag': 'PropertyPlantAndEquipmentNet'},
    
        # Income Statement
        'COGS': {'definition': 'Cost of Goods Sold', 'type': 'float', 'xbrl_tag': 'CostOfGoodsSold'},
        'EBIT': {'definition': 'Earnings Before Interest and Taxes', 'type': 'float', 'xbrl_tag': 'OperatingIncomeLoss'},
        'EBITDA': {'definition': 'Earnings Before Interest, Taxes, Depreciation, and Amortization', 'type': 'float', 'xbrl_tag': 'EBITDA'},
        'GP': {'definition': 'Gross Profit', 'type': 'float', 'xbrl_tag': 'GrossProfit'},
        'IB': {'definition': 'Income Before Tax', 'type': 'float', 'xbrl_tag': 'IncomeBeforeTax'},
        'NI': {'definition': 'Net Income', 'type': 'float', 'xbrl_tag': 'NetIncomeLoss'},
        'REVT': {'definition': 'Revenue Total', 'type': 'float', 'xbrl_tag': 'Revenues'},
        'SALE': {'definition': 'Sales/Revenue', 'type': 'float', 'xbrl_tag': 'SalesRevenueNet'},
    
        # Tax and Operating Expenses
        'TXT': {'definition': 'Income Taxes', 'type': 'float', 'xbrl_tag': 'IncomeTaxExpenseBenefit'},
        'XAD': {'definition': 'Advertising Expense', 'type': 'float', 'xbrl_tag': 'AdvertisingExpense'},
        'XRD': {'definition': 'Research & Development Expenses', 'type': 'float', 'xbrl_tag': 'ResearchAndDevelopmentExpense'},
        'XSGA': {'definition': 'Selling, General, and Admin Expenses', 'type': 'float', 'xbrl_tag': 'SellingGeneralAndAdministrativeExpense'},
    
        # Location Information
        'ADD1': {'definition': 'Address Line 1', 'type': 'text', 'xbrl_tag': 'AddressLine1'},
        'ADDZIP': {'definition': 'Zip Code', 'type': 'text', 'xbrl_tag': 'ZipCode'},
        'CITY': {'definition': 'City', 'type': 'text', 'xbrl_tag': 'City'},
    
        # Investments and Intangibles
        'INTAN': {'definition': 'Intangible Assets', 'type': 'float', 'xbrl_tag': 'IntangibleAssetsNet'},
        'UINVT': {'definition': 'Unconsolidated Investments', 'type': 'float', 'xbrl_tag': 'InvestmentsInUnconsolidatedEntities'},
        
        # Financial Ratios
        'TIE': {'definition': 'Times Interest Earned', 'type': 'float', 'xbrl_tag': 'InterestExpense'},
        'TXC': {'definition': 'Current Taxes', 'type': 'float', 'xbrl_tag': 'TaxesCurrent'},
        'XT': {'definition': 'Total Expenses', 'type': 'float', 'xbrl_tag': 'Expenses'},
    
        # Changes in Inventory and Acquisition
        'INVCH': {'definition': 'Change in Inventory', 'type': 'float', 'xbrl_tag': 'InventoryChange'},
        'ACQINVT': {'definition': 'Acquisition of Inventory', 'type': 'float', 'xbrl_tag': 'InventoryAcquisition'},

        # Cash Flow Variables
        'DPC': {'definition': 'Depreciation and Amortization', 'type': 'float', 'xbrl_tag': 'DepreciationAndAmortization'},
        'FIC': {'definition': 'Financing Cash Flow', 'type': 'float', 'xbrl_tag': 'NetCashProvidedByUsedInFinancingActivities'},
        'OIBDP': {'definition': 'Operating Income Before Depreciation', 'type': 'float', 'xbrl_tag': 'OperatingIncomeBeforeDepreciation'},
        'OIADP': {'definition': 'Operating Income After Depreciation', 'type': 'float', 'xbrl_tag': 'OperatingIncomeAfterDepreciation'},
        'CFO': {'definition': 'Cash Flow from Operating Activities', 'type': 'float', 'xbrl_tag': 'NetCashProvidedByUsedInOperatingActivities'},
        'CFI': {'definition': 'Cash Flow from Investing Activities', 'type': 'float', 'xbrl_tag': 'NetCashProvidedByUsedInInvestingActivities'},
        'CFF': {'definition': 'Cash Flow from Financing Activities', 'type': 'float', 'xbrl_tag': 'NetCashProvidedByUsedInFinancingActivities'},
        'CAPX': {'definition': 'Capital Expenditures', 'type': 'float', 'xbrl_tag': 'CapitalExpenditures'},
        'FCF': {'definition': 'Free Cash Flow', 'type': 'float', 'xbrl_tag': 'FreeCashFlow'},
        'CH': {'definition': 'Change in Cash', 'type': 'float', 'xbrl_tag': 'CashAndCashEquivalentsPeriodIncreaseDecrease'},
        'NCC': {'definition': 'Net Change in Cash', 'type': 'float', 'xbrl_tag': 'NetIncreaseDecreaseInCashAndCashEquivalents'},
        'DVC': {'definition': 'Dividends Paid', 'type': 'float', 'xbrl_tag': 'PaymentsOfDividendsCommonStock'},
        'SPPE': {'definition': 'Sale of Property, Plant, and Equipment', 'type': 'float', 'xbrl_tag': 'ProceedsFromSaleOfPropertyPlantAndEquipment'},
        'APPE': {'definition': 'Acquisition of Property, Plant, and Equipment', 'type': 'float', 'xbrl_tag': 'PaymentsToAcquirePropertyPlantAndEquipment'},
        'SSTK': {'definition': 'Sale of Stock', 'type': 'float', 'xbrl_tag': 'ProceedsFromIssuanceOfCommonStock'},
        'PRSTK': {'definition': 'Purchase of Stock', 'type': 'float', 'xbrl_tag': 'PaymentsForRepurchaseOfCommonStock'},
        'DLTIS': {'definition': 'Debt Issued', 'type': 'float', 'xbrl_tag': 'ProceedsFromIssuanceOfLongTermDebt'},
        'DLTR': {'definition': 'Debt Repayment', 'type': 'float', 'xbrl_tag': 'RepaymentsOfLongTermDebt'},
        'IVNCF': {'definition': 'Investing Cash Flow, Net', 'type': 'float', 'xbrl_tag': 'NetCashProvidedByUsedInInvestingActivities'},
        'FNCF': {'definition': 'Financing Cash Flow, Net', 'type': 'float', 'xbrl_tag': 'NetCashProvidedByUsedInFinancingActivities'},
        'OPNCF': {'definition': 'Operating Cash Flow, Net', 'type': 'float', 'xbrl_tag': 'NetCashProvidedByUsedInOperatingActivities'}
    }

    def __init__(self, user_agent: str):
        self.headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate'
        }
        self.company_tickers_url = "https://www.sec.gov/files/company_tickers.json"
        self.company_facts_url = "https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sec_edgar_compustat.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_all_companies(self) -> Dict[str, str]:
        try:
            response = requests.get(self.company_tickers_url, headers=self.headers)
            response.raise_for_status()
            companies_data = response.json()
            return {str(company['cik_str']).zfill(10): company['title'] for company in companies_data.values()}
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching company list: {str(e)}")
            return {}

    def find_most_recent_10k(self, values: List[Dict]) -> Optional[Dict]:
        """
        Find the most recent 10-K filing from a list of values
        """
        ten_k_values = [v for v in values if v.get('form') == '10-K']
        if not ten_k_values:
            return None
        return max(ten_k_values, key=lambda x: x['end'])

    def get_value_for_year(self, data: Dict, tag: str, year: int, var_type: str) -> Optional[any]:
        """
        Extract value for a specific XBRL tag and year with improved logic
        """
        try:
            # Check both us-gaap and dei namespaces
            namespaces = ['us-gaap', 'dei']
            
            for namespace in namespaces:
                if namespace not in data:
                    continue
                    
                if tag not in data[namespace]:
                    continue

                tag_data = data[namespace][tag]
                
                # Handle different unit types
                if var_type == 'float':
                    units_key = 'USD'
                elif var_type == 'integer':
                    units_key = 'pure'
                else:  # text type
                    units_key = None
                
                # Get all units
                units = tag_data.get('units', {})
                
                # For text fields, try to get the value directly
                if units_key is None:
                    if 'string' in units:
                        values = units['string']
                    else:
                        continue
                else:
                    if units_key not in units:
                        continue
                    values = units[units_key]

                # Filter values for the specific year
                year_values = [
                    v for v in values 
                    if v.get('form') == '10-K' 
                    and v['end'].startswith(str(year))
                ]

                if year_values:
                    # Get the most recent value for that year
                    value = max(year_values, key=lambda x: x['end'])['val']
                    
                    # Convert to appropriate type
                    if var_type == 'float':
                        return float(value)
                    elif var_type == 'integer':
                        return int(float(value))  # Handle cases where integers are reported as floats
                    else:
                        return str(value)
                        
        except Exception as e:
            self.logger.debug(f"Error extracting {tag} for year {year}: {str(e)}")
            return None
            
        return None

    def process_company_data(self, cik: str, company_name: str) -> pd.DataFrame:
        """
        Process company data with improved error handling and data extraction
        """
        self.logger.info(f"Processing data for {company_name} (CIK: {cik})")
        
        try:
            url = self.company_facts_url.format(cik)
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            if not data or 'facts' not in data:
                self.logger.warning(f"No facts found for {company_name}")
                return pd.DataFrame()
            
            processed_data = []
            
            for year in [2022, 2023]:
                # Initialize year_data with all variables set to None
                year_data = {var_name: None for var_name in self.VARIABLE_DEFINITIONS.keys()}
                
                # Set the basic identifiers
                year_data.update({
                    'CIK': cik,
                    'CONM': company_name,
                    'FYEAR': year
                })
                
                # Track if we found any data for this year
                found_data = False
                
                for var_name, var_info in self.VARIABLE_DEFINITIONS.items():
                    if var_name not in ['CIK', 'CONM', 'FYEAR']:
                        xbrl_tag = var_info['xbrl_tag']
                        var_type = var_info['type']
                        
                        value = self.get_value_for_year(data['facts'], xbrl_tag, year, var_type)
                        if value is not None:
                            year_data[var_name] = value
                            found_data = True
                
                # Only add the year's data if we found at least some values
                if found_data:
                    processed_data.append(year_data)
            
            if processed_data:
                return pd.DataFrame(processed_data)
            else:
                self.logger.warning(f"No valid data found for {company_name}")
                return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error processing data for {company_name}: {str(e)}")
            return pd.DataFrame()

    def fetch_all_company_data(self, max_workers: int = 5, max_companies: Optional[int] = None):
        """
        Fetch and process data for all companies with improved error handling and rate limiting
        """
        companies = self.get_all_companies()
        if not companies:
            self.logger.error("Failed to fetch company list")
            return
        
        if max_companies:
            companies = dict(list(companies.items())[:max_companies])
        
        all_data = []
        processed_companies = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_company = {
                executor.submit(self.process_company_data, cik, name): (cik, name)
                for cik, name in companies.items()
            }
            
            for future in future_to_company:
                cik, name = future_to_company[future]
                try:
                    df = future.result()
                    if not df.empty:
                        all_data.append(df)
                        processed_companies += 1
                        if processed_companies % 10 == 0:
                            self.logger.info(f"Processed {processed_companies} companies")
                except Exception as e:
                    self.logger.error(f"Error processing {name}: {str(e)}")
                
                # Rate limiting
                time.sleep(0.1)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Remove duplicate rows
            combined_df = combined_df.drop_duplicates()
            
            # Sort by company name and year
            combined_df = combined_df.sort_values(['CONM', 'FYEAR'])
            
            output_file = f"sec_edgar_compustat_data_{datetime.now().strftime('%Y%m%d')}.csv"
            combined_df.to_csv(output_file, index=False)
            
            # Log statistics
            total_companies = len(companies)
            successful_companies = len(combined_df['CIK'].unique())
            total_vars = len(self.VARIABLE_DEFINITIONS)
            collected_vars = len([col for col in combined_df.columns if col in self.VARIABLE_DEFINITIONS])
            
            self.logger.info(f"Data collection completed:")
            self.logger.info(f"- Successfully processed {successful_companies} out of {total_companies} companies")
            self.logger.info(f"- Collected {collected_vars} out of {total_vars} defined variables")
            self.logger.info(f"- Total rows in dataset: {len(combined_df)}")
            self.logger.info(f"- Data saved to {output_file}")
        else:
            self.logger.error("No data collected")

def main():
    user_agent = "USER (USERNAME@gmail.com)"  # Replace with your information
    fetcher = SECEdgarCompustatFetcher(user_agent)
    fetcher.fetch_all_company_data(max_workers=5, max_companies=None)

if __name__ == "__main__":
    main()