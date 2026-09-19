// PATH: server.js
const express = require("express");
const path = require("path");
const app = express();

// Deployment Cost Limit: ₹0
app.use(express.static(path.join(__dirname, "public")));

app.get("/user/:id", (req, res) => {
    const id = req.params.id;
    res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/check/:phone", (req, res) => {
    const phone = req.params.phone;
    res.sendFile(path.join(__dirname, "public", "index.html"));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});