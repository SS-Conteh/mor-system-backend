require("dotenv").config();
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");

async function seed() {
  const uri = process.env.MONGODB_URI || process.env.MONGO_URI;
  if (!uri) throw new Error("Set MONGODB_URI (or MONGO_URI) in .env before running the seed.");

  await mongoose.connect(uri);
  // This intentionally removes every collection in the configured database.
  await mongoose.connection.db.dropDatabase();

  const password = await bcrypt.hash("Admin123", 10);
  await mongoose.connection.collection("users").insertOne({
    fullName: "Marcus D. Williams",
    phoneNumber: "076931955",
    password,
    role: "Head Shepherd",
    branch: "MOR Head Quarter",
    group: null,
    membershipStatus: "Leader",
    approvalStatus: "approved",
    isHeadShepherd: true,
    createdAt: new Date(),
    lastLogin: null,
  });

  console.log("System cleared. Head Shepherd account created for Marcus D. Williams.");
  await mongoose.disconnect();
}

seed().catch(async (error) => {
  console.error(error.message);
  await mongoose.disconnect().catch(() => {});
  process.exit(1);
});
