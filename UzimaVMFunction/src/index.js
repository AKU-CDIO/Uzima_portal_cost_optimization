const { DefaultAzureCredential } = require('@azure/identity');
const { ComputeManagementClient } = require('@azure/arm-compute');

module.exports = async function (context, req) {
    context.log('HTTP trigger function processed a request.');
    
    const subscriptionId = process.env.AZURE_SUBSCRIPTION_ID;
    const resourceGroup = process.env.RESOURCE_GROUP_NAME;
    const vmName = process.env.VM_NAME;

    try {
        const credential = new DefaultAzureCredential();
        const client = new ComputeManagementClient(credential, subscriptionId);
        
        const vm = await client.virtualMachines.get(resourceGroup, vmName, { expand: 'instanceView' });
        const status = vm.instanceView.statuses[1].displayStatus.toLowerCase();
        
        return {
            status: 200,
            body: { status },
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        };
    } catch (error) {
        context.log.error('Error:', error);
        return {
            status: 500,
            body: { 
                error: error.message,
                details: error.toString()
            },
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        };
    }
};