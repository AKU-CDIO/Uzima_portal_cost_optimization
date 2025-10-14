const { DefaultAzureCredential } = require("@azure/identity");
const { ComputeManagementClient } = require("@azure/arm-compute");

module.exports = async function (context, req) {
    try {
        const subscriptionId = process.env.AZURE_SUBSCRIPTION_ID;
        const resourceGroup = process.env.AZURE_RESOURCE_GROUP;
        const vmName = process.env.AZURE_VM_NAME;
        
        const credential = new DefaultAzureCredential();
        const client = new ComputeManagementClient(credential, subscriptionId);
        
        const vm = await client.virtualMachines.get(resourceGroup, vmName, { expand: 'instanceView' });
        const isRunning = vm.instanceView.statuses.some(
            status => status.code === 'PowerState/running'
        );
        
        return {
            status: 200,
            body: { isRunning }
        };
    } catch (error) {
        context.log.error('Error checking VM status:', error);
        return {
            status: 500,
            body: { success: false, error: error.message, isRunning: false }
        };
    }
};
