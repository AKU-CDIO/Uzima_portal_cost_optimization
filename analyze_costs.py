import pandas as pd
import matplotlib.pyplot as plt

def load_and_analyze_cost_data(file_path):
    # Load the CSV file
    df = pd.read_csv(file_path)
    
    # Convert CostUSD to numeric (in case it's not already)
    df['CostUSD'] = pd.to_numeric(df['CostUSD'], errors='coerce')
    
    # 1. Total cost by resource type
    resource_costs = df.groupby('ResourceType')['CostUSD'].sum().sort_values(ascending=False)
    
    # 2. Total cost by service name
    service_costs = df.groupby('ServiceName')['CostUSD'].sum().sort_values(ascending=False)
    
    # 3. Top expensive resources
    top_resources = df.groupby('Resource')['CostUSD'].sum().sort_values(ascending=False).head(10)
    
    # 4. Cost by meter category
    meter_costs = df.groupby('Meter')['CostUSD'].sum().sort_values(ascending=False).head(15)
    
    # 5. Cost by resource group (though most seem to be in cdiouzima)
    rg_costs = df.groupby('ResourceGroupName')['CostUSD'].sum().sort_values(ascending=False)
    
    return {
        'total_cost': df['CostUSD'].sum(),
        'by_resource_type': resource_costs,
        'by_service': service_costs,
        'top_resources': top_resources,
        'by_meter': meter_costs,
        'by_resource_group': rg_costs,
        'raw_data': df
    }

def generate_cost_report(analysis):
    print("\n=== Azure Cost Analysis Report ===\n")
    print(f"Total Monthly Cost: ${analysis['total_cost']:,.2f}\n")
    
    print("\n--- Costs by Resource Type ---")
    print(analysis['by_resource_type'].to_string())
    
    print("\n--- Top 10 Most Expensive Resources ---")
    print(analysis['top_resources'].to_string())
    
    print("\n--- Top 15 Costly Meter Categories ---")
    print(analysis['by_meter'].to_string())
    
    print("\n--- Costs by Service ---")
    print(analysis['by_service'].to_string())
    
    print("\n--- Costs by Resource Group ---")
    print(analysis['by_resource_group'].to_string())

def plot_cost_distribution(analysis):
    plt.figure(figsize=(12, 6))
    analysis['by_service'].plot(kind='bar')
    plt.title('Cost Distribution by Service')
    plt.ylabel('Cost (USD)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('cost_distribution.png')
    print("\nSaved cost distribution chart as 'cost_distribution.png'")

if __name__ == "__main__":
    file_path = 'CostManagement_CDIOUZIMA_2025-10-02-1759379028787.csv'
    try:
        print(f"Analyzing cost data from {file_path}...")
        analysis = load_and_analyze_cost_data(file_path)
        generate_cost_report(analysis)
        plot_cost_distribution(analysis)
    except Exception as e:
        print(f"Error analyzing cost data: {str(e)}")
