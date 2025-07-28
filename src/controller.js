const Records = require("./records.model");
const path = require("path");
const fs = require("fs");
const csv = require("csv-parser");

const upload = async (req, res) => {
  const { file } = req;

  if (!file) {
    return res.status(400).json({ message: "No file uploaded." });
  }
  const filePath = path.resolve(__dirname, "..", "_temp", file.filename);
  const BATCH_SIZE = 20000;
  const batch = [];

  try {
    await new Promise((resolve, reject) => {
      const stream = fs
        .createReadStream(filePath)
        .pipe(csv())
        .on("data", async (data) => {
          batch.push(data);

          if (batch.length >= BATCH_SIZE) {
            stream.pause();
            try {
              await Records.insertMany(batch);
              batch.length = 0;
            } catch (err) {
              return reject(err);
            } finally {
              stream.resume();
            }
          }
        })
        .on("end", async () => {
          try {
            if (batch.length > 0) {
              await Records.insertMany(batch);
            }
            resolve();
          } catch (err) {
            reject(err);
          }
        })
        .on("error", reject);
    });

    fs.unlinkSync(filePath);

    return res
      .status(200)
      .json({ message: "File processed and uploaded successfully" });
  } catch (error) {
    console.error("Error  processing CSV:", error);
    return res.status(500).json({ error: "Error processing the file" });
  }
};

const list = async (_, res) => {
  try {
    const data = await Records.find({}).limit(10).lean();

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json(err);
  }
};

module.exports = {
  upload,
  list,
};
