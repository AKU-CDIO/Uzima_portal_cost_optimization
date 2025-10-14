const { DefaultAzureCredential } = require("@azure/identity");
const { ComputeManagementClient } = require("@azure/arm-compute");

module.exports = async function (context, req) {
    try {
        const subscriptionId = process.env.AZURE_SUBSCRIPTION_ID;
        const resourceGroup = process.env.AZURE_RESOURCE_GROUP;
        const vmName = process.env.AZURE_VM_NAME;
        
        const credential = new DefaultAzureCredential();
        const client = new ComputeManagementClient(credential, subscriptionId);
        
        await client.virtualMachines.beginDeallocate(resourceGroup, vmName);
        
        return {
            status: 200,
            body: { success: true, message: "VM stop initiated" }
        };
    } catch (error) {
        context.log.error('Error stopping VM:', error);
        return {
            status: 500,
            body: { success: false, error: error.message }
        };
    }
};
