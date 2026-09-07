/**
 * seed.js
 * ─────────────────────────────────────────────────────────────────────────
 * Wipes EVERY collection in the MOR System database and creates a single
 * Head Shepherd account to bootstrap the system from a clean slate.
 *
 * ⚠️  DESTRUCTIVE — this deletes all members, attendance, groups, branches,
 *     notifications, everything. There is no undo. It asks for a typed
 *     confirmation unless you pass --force.
 *
 * USAGE
 * ─────
 *   node seed.js
 *       Interactive — prompts for the Head Shepherd's name, phone number
 *       and password, then asks you to type CLEAR to confirm the wipe.
 *
 *   node seed.js --name="John Doe" --phone="+23276000000" --password="secret123"
 *       Non-interactive values via flags (still asks for the CLEAR
 *       confirmation unless --force is also passed).
 *
 *   node seed.js --name="John Doe" --phone="+23276000000" --password="secret123" --force
 *       Fully non-interactive — use in scripts/CI with care.
 *
 * You can also set SEED_HEAD_SHEPHERD_NAME / SEED_HEAD_SHEPHERD_PHONE /
 * SEED_HEAD_SHEPHERD_PASSWORD as environment variables instead of flags.
 */

require("dotenv").config();
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const readline = require("readline");

// ── Parse CLI flags (--name="x" / --name x) ──
function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const eq = a.indexOf("=");
    if (eq !== -1) {
      out[a.slice(2, eq)] = a.slice(eq + 1);
    } else {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (next && !next.startsWith("--")) {
        out[key] = next;
        i++;
      } else {
        out[key] = true;
      }
    }
  }
  return out;
}
const args = parseArgs(process.argv.slice(2));
const FORCE = args.force === true || args.force === "true";

function ask(question) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

// ── Minimal User model — only the fields the seed needs to write.
//    This is intentionally standalone (does not require server.js) so
//    running the seed never boots the HTTP server or its listeners. ──
const UserSchema = new mongoose.Schema({
  fullName: { type: String, required: true },
  phoneNumber: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  role: { type: String, default: "Head Shepherd" },
  branch: { type: String, default: "MOR Head Quarter" },
  group: { type: String, default: null },
  membershipStatus: { type: String, default: "Leader" },
  approvalStatus: { type: String, default: "approved" },
  isHeadShepherd: { type: Boolean, default: true },
  createdAt: { type: Date, default: Date.now },
});
const User = mongoose.model("User", UserSchema);

async function wipeDatabase() {
  const db = mongoose.connection.db;
  const collections = await db.listCollections().toArray();
  if (!collections.length) {
    console.log("   (database already empty)");
    return;
  }
  for (const { name } of collections) {
    // Skip Mongo's internal system collections just in case.
    if (name.startsWith("system.")) continue;
    await db.collection(name).deleteMany({});
    console.log(`   ✓ Cleared collection: ${name}`);
  }
}

async function main() {
  console.log("🌱 MOR System — Database Seed Script");
  console.log("=====================================");

  const name =
    args.name || process.env.SEED_HEAD_SHEPHERD_NAME || (await ask("Head Shepherd full name: "));
  const phone =
    args.phone ||
    process.env.SEED_HEAD_SHEPHERD_PHONE ||
    (await ask("Head Shepherd phone number (login ID): "));
  const password =
    args.password ||
    process.env.SEED_HEAD_SHEPHERD_PASSWORD ||
    (await ask("Head Shepherd password (min 6 characters): "));

  if (!name || !phone || !password) {
    console.error("❌ Name, phone number and password are all required.");
    process.exit(1);
  }
  if (password.length < 6) {
    console.error("❌ Password must be at least 6 characters.");
    process.exit(1);
  }

  if (!FORCE) {
    console.log(
      "\n⚠️  This will PERMANENTLY DELETE every record in the database",
    );
    console.log(
      "    (members, attendance, groups, branches, notifications, etc.)",
    );
    const confirm = await ask('   Type "CLEAR" to continue: ');
    if (confirm !== "CLEAR") {
      console.log("Aborted — nothing was changed.");
      process.exit(0);
    }
  }

  console.log("\n🔌 Connecting to MongoDB...");
  await mongoose.connect(process.env.MONGODB_URI, {
    serverSelectionTimeoutMS: 8000,
  });
  console.log("✅ Connected.");

  console.log("\n🧹 Wiping all collections...");
  await wipeDatabase();

  console.log("\n👑 Creating Head Shepherd account...");
  const hashedPassword = await bcrypt.hash(password, 10);
  const headShepherd = await User.create({
    fullName: name,
    phoneNumber: phone,
    password: hashedPassword,
    role: "Head Shepherd",
    branch: "MOR Head Quarter",
    group: null,
    membershipStatus: "Leader",
    approvalStatus: "approved",
    isHeadShepherd: true,
  });
  console.log(`   ✓ Created: ${headShepherd.fullName} (${headShepherd.phoneNumber})`);

  console.log("\n✅ Done! The database is clean and the Head Shepherd account is ready.");
  console.log(
    "   Note: branches, ministry groups and reminder schedules will be re-seeded",
    "\n   automatically the next time the backend server starts (initializeDatabase()).",
  );

  await mongoose.disconnect();
  process.exit(0);
}

main().catch((err) => {
  console.error("❌ Seed failed:", err.message);
  process.exit(1);
});
