const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
const PORT = 3000;

// Enable CORS
app.use(cors());
app.use(express.json());

// Proxy endpoint
app.post('/api/proxy', async (req, res) => {
    try {
        const { url } = req.body;
        const response = await axios.post(url, {}, {
            headers: {
                'Content-Type': 'application/json'
            }
        });
        res.json({ success: true, data: response.data });
    } catch (error) {
        console.error('Proxy error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Serve static files
app.use(express.static('public'));

app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});