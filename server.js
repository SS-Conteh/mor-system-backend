require("dotenv").config();
const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const path = require("path");
const multer = require("multer");
const fs = require("fs");
const cloudinary = require("cloudinary").v2;
const crypto = require("crypto");
const cron = require("node-cron");

// ========== CLOUDINARY CONFIGURATION ==========
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const app = express();

// ========== MIDDLEWARE ==========
app.use(
  cors({
    origin: [
      "http://localhost:3000",
      "http://127.0.0.1:5500",
      "http://localhost:5500",
      "http://localhost:5000",
      "https://mor-system-app.vercel.app",
      "https://mor-system-grhjve1h3-ss-conteh.vercel.app",
      "http://localhost",
      "capacitor://localhost",
    ],
    credentials: true,
  }),
);
app.use(express.json());
app.use(express.static(path.join(__dirname, "../frontend")));

// ========== DATABASE MODELS ==========

const MEMBERSHIP_STATUSES = [
  "First Timer",
  "Inconsistent",
  "Semi-Consistent",
  "Consistent",
  "Intense Leader",
  "Leader",
];

// User Model
const UserSchema = new mongoose.Schema({
  fullName: { type: String, required: true },
  phoneNumber: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  role: {
    type: String,
    enum: [
      "Head Shepherd",
      "Branch Head Shepherd",
      "Group Leader",
      "Member",
      "System Admin",
    ],
    default: "Member",
  },
  branch: { type: String, default: "MOR Head Quarter" },
  group: { type: String, default: null },
  profilePhoto: { type: String, default: "" },
  dateOfBirth: Date,
  gender: String,
  address: String,
  occupation: String,
  school: String,
  church: String,
  cbsLocation: String,
  membershipStatus: {
    type: String,
    enum: MEMBERSHIP_STATUSES,
    default: "First Timer",
  },
  isSteward: { type: Boolean, default: false },
  stewardSince: Date,
  isCBSLeader: { type: Boolean, default: false },
  assignedCBSLocation: { type: String, default: null },
  isGroupLeader: { type: Boolean, default: false },
  isHeadShepherd: { type: Boolean, default: false },
  isSystemAdmin: { type: Boolean, default: false },
  isBranchShepherd: { type: Boolean, default: false },
  assignedMembers: [{ type: mongoose.Schema.Types.ObjectId, ref: "Member" }],
  assignedTo: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    default: null,
  },
  assignedToName: { type: String, default: null },
  createdAt: { type: Date, default: Date.now },
  lastLogin: Date,
});

// Member Model
const MemberSchema = new mongoose.Schema({
  fullName: { type: String, required: true },
  phoneNumber: { type: String, required: true, unique: true },
  email: String,
  profilePhoto: { type: String, default: "" },
  dateOfBirth: Date,
  gender: String,
  address: String,
  occupation: String,
  school: String,
  church: String,
  branch: { type: String, default: "MOR Head Quarter" },
  group: { type: String, default: null },
  cbsLocation: String,
  membershipStatus: {
    type: String,
    enum: MEMBERSHIP_STATUSES,
    default: "First Timer",
  },
  // ── FIX #3: Added role field to Member model ──
  role: {
    type: String,
    enum: ["Head Shepherd", "Branch Head Shepherd", "Group Leader", "Member"],
    default: "Member",
  },
  isSteward: { type: Boolean, default: false },
  stewardSince: Date,
  isCBSLeader: { type: Boolean, default: false },
  assignedCBSLocation: { type: String, default: null },
  isGroupLeader: { type: Boolean, default: false },
  isBranchShepherd: { type: Boolean, default: false },
  assignedTo: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    default: null,
  },
  assignedToName: { type: String, default: null },
  addedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  addedByName: String,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});

// Attendance Model
const AttendanceSchema = new mongoose.Schema({
  type: {
    type: String,
    enum: ["fellowship", "cbs", "evangelism", "seminar"],
    required: true,
  },
  branch: { type: String, default: null },
  group: { type: String, default: null },
  cbsLocation: String,
  date: { type: Date, required: true },
  records: [
    {
      memberId: { type: mongoose.Schema.Types.ObjectId, ref: "Member" },
      memberName: String,
      status: { type: String, enum: ["present", "absent"], default: "absent" },
      checkInTime: Date,
      scanMethod: { type: String, enum: ["manual", "qr"], default: "manual" },
    },
  ],
  stats: { total: Number, present: Number, absent: Number, percentage: Number },
  recordedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  recordedByName: String,
  createdAt: { type: Date, default: Date.now },
});

// Group Model
const GroupSchema = new mongoose.Schema({
  name: { type: String, required: true, unique: true },
  branch: { type: String, default: "MOR Head Quarter" },
  leader: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  leaderName: String,
  leaderPhone: String,
  assistantLeader: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  assistantLeaderName: String,
  assistantLeaderPhone: String,
  memberCount: { type: Number, default: 0 },
  stewardCount: { type: Number, default: 0 },
  intenseLeaderCount: { type: Number, default: 0 },
  consistentCount: { type: Number, default: 0 },
  semiConsistentCount: { type: Number, default: 0 },
  inconsistentCount: { type: Number, default: 0 },
  firstTimerCount: { type: Number, default: 0 },
  description: String,
  isActive: { type: Boolean, default: true },
  createdAt: { type: Date, default: Date.now },
});

// CBS Location Model
const CBSLocationSchema = new mongoose.Schema({
  name: { type: String, required: true, unique: true },
  branch: { type: String, default: "MOR Head Quarter" },
  leader: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  leaderName: String,
  leaderPhone: String,
  associatedGroups: [{ type: String }],
  memberCount: { type: Number, default: 0 },
  status: {
    type: String,
    enum: ["Active", "Pending", "Inactive"],
    default: "Pending",
  },
  createdAt: { type: Date, default: Date.now },
});

// Branch Model
const BranchSchema = new mongoose.Schema({
  name: { type: String, required: true, unique: true },
  headShepherd: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    default: null,
  },
  headShepherdName: { type: String, default: null },
  headShepherdPhone: { type: String, default: null },
  memberCount: { type: Number, default: 0 },
  groupCount: { type: Number, default: 0 },
  description: String,
  isActive: { type: Boolean, default: true },
  createdAt: { type: Date, default: Date.now },
});

// QR Session Model
const QRSessionSchema = new mongoose.Schema({
  token: { type: String, required: true, unique: true },
  type: {
    type: String,
    enum: ["fellowship", "cbs", "evangelism"],
    required: true,
  },
  branch: { type: String, default: "MOR Head Quarter" },
  group: String,
  cbsLocation: String,
  date: { type: Date, required: true },
  expiresAt: { type: Date, required: true },
  createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  createdByName: String,
  isActive: { type: Boolean, default: true },
  scans: [
    {
      memberId: { type: mongoose.Schema.Types.ObjectId, ref: "Member" },
      memberName: String,
      group: String,
      branch: { type: String, default: "MOR Head Quarter" },
      membershipStatus: String,
      scanTime: { type: Date, default: Date.now },
      timing: { type: String, enum: ["on_time", "late"], default: "on_time" },
    },
  ],
  createdAt: { type: Date, default: Date.now },
});

// Follow-up Assignment Model
const AssignmentSchema = new mongoose.Schema({
  assignedBy: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    required: true,
  },
  assignedByName: String,
  assignedTo: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    required: true,
  },
  assignedToName: String,
  assignedToRole: String,
  assignedToStatus: String,
  member: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Member",
    required: true,
  },
  memberName: String,
  memberPhone: String,
  memberStatus: String,
  group: String,
  branch: { type: String, default: "MOR Head Quarter" },
  notes: String,
  createdAt: { type: Date, default: Date.now },
});

// Report Model
const ReportSchema = new mongoose.Schema({
  title: { type: String, required: true },
  type: {
    type: String,
    enum: ["weekly", "monthly", "quarterly", "manual", "inconsistency", "auto"],
    required: true,
  },
  scope: {
    type: String,
    enum: ["general", "group", "branch"],
    default: "general",
  },
  targetGroup: String,
  targetBranch: String,
  sentTo: [
    { userId: mongoose.Schema.Types.ObjectId, name: String, role: String },
  ],
  sentBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  sentByName: String,
  body: String,
  data: mongoose.Schema.Types.Mixed,
  period: { start: Date, end: Date },
  isRead: { type: Boolean, default: false },
  readBy: [{ type: mongoose.Schema.Types.ObjectId, ref: "User" }],
  createdAt: { type: Date, default: Date.now },
});

// Notification Schedule Model
const NotifScheduleSchema = new mongoose.Schema({
  title: { type: String, required: true },
  message: { type: String, required: true },
  activityType: {
    type: String,
    enum: ["fellowship", "cbs", "evangelism", "seminar", "camp", "general"],
    default: "general",
  },
  schedule: {
    dayOfWeek: Number,
    weekPattern: {
      type: String,
      enum: ["every", "first", "second", "third", "fourth", "last"],
      default: "every",
    },
    month: Number,
    hourUTC: { type: Number, default: 8 },
  },
  isActive: { type: Boolean, default: true },
  targetScope: {
    type: String,
    enum: ["all", "group", "branch"],
    default: "all",
  },
  targetGroup: String,
  targetBranch: String,
  lastSent: Date,
  createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  createdAt: { type: Date, default: Date.now },
});

// Notification Model
const NotificationSchema = new mongoose.Schema({
  title: { type: String, required: true },
  message: { type: String, required: true },
  type: {
    type: String,
    enum: ["general", "group", "personal", "report", "reminder"],
    default: "general",
  },
  targetGroup: String,
  targetBranch: String,
  targetUser: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  sentBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  sentByName: String,
  sentByRole: String,
  readBy: [{ type: mongoose.Schema.Types.ObjectId, ref: "User" }],
  isUrgent: { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now },
});

// Activity Log Model
const ActivityLogSchema = new mongoose.Schema({
  action: { type: String, required: true },
  user: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  userName: String,
  userRole: String,
  branch: { type: String, default: null },
  details: String,
  ipAddress: String,
  createdAt: { type: Date, default: Date.now },
});

// Media Model
const MediaSchema = new mongoose.Schema({
  title: { type: String, required: true },
  type: {
    type: String,
    enum: ["audio", "video", "doc", "image"],
    required: true,
  },
  description: String,
  fileName: String,
  filePath: String,
  fileSize: Number,
  mimeType: String,
  branch: { type: String, default: null },
  uploadedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  uploadedByName: String,
  createdAt: { type: Date, default: Date.now },
});

// Create Models
const User = mongoose.model("User", UserSchema);
const Member = mongoose.model("Member", MemberSchema);
const Attendance = mongoose.model("Attendance", AttendanceSchema);
const Group = mongoose.model("Group", GroupSchema);
const CBSLocation = mongoose.model("CBSLocation", CBSLocationSchema);
const Branch = mongoose.model("Branch", BranchSchema);
const QRSession = mongoose.model("QRSession", QRSessionSchema);
const Assignment = mongoose.model("Assignment", AssignmentSchema);
const Report = mongoose.model("Report", ReportSchema);
const NotifSchedule = mongoose.model("NotifSchedule", NotifScheduleSchema);
const Notification = mongoose.model("Notification", NotificationSchema);
const ActivityLog = mongoose.model("ActivityLog", ActivityLogSchema);
const Media = mongoose.model("Media", MediaSchema);
// FollowUp Chat Model
const FollowUpChatSchema = new mongoose.Schema({
  assignmentId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Assignment",
    required: true,
  },
  fromMemberId: { type: mongoose.Schema.Types.ObjectId, required: true },
  fromName: { type: String, required: true },
  fromRole: { type: String, default: "Member" }, // "Leader","Steward","Member"
  toMemberId: { type: mongoose.Schema.Types.ObjectId, required: true },
  toName: { type: String },
  message: { type: String, required: true },
  readBy: [{ type: mongoose.Schema.Types.ObjectId }],
  createdAt: { type: Date, default: Date.now },
});
const FollowUpChat = mongoose.model("FollowUpChat", FollowUpChatSchema);

// Bulk Import Log Model — tracks every excel upload by group leaders
const BulkImportSchema = new mongoose.Schema({
  uploadedBy: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    required: true,
  },
  uploadedByName: { type: String, required: true },
  group: { type: String, required: true },
  branch: { type: String, default: "MOR Head Quarter" },
  fileName: { type: String, required: true },
  totalRows: { type: Number, default: 0 },
  successCount: { type: Number, default: 0 },
  skippedCount: { type: Number, default: 0 },
  skippedDetails: [{ phone: String, name: String, reason: String }],
  createdAt: { type: Date, default: Date.now },
});
const BulkImport = mongoose.model("BulkImport", BulkImportSchema);

// ========== STATUS UPDATE MODEL ==========
// Tracks every automatic or pending status change
const StatusUpdateSchema = new mongoose.Schema({
  memberId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Member",
    required: true,
  },
  memberName: { type: String, required: true },
  memberPhone: String,
  group: String,
  branch: { type: String, default: "MOR Head Quarter" },
  fromStatus: { type: String, required: true },
  toStatus: { type: String, required: true },
  direction: { type: String, enum: ["up", "down"], required: true },
  attendancePct: { type: Number, required: true }, // e.g. 66.7
  attended: { type: Number, required: true },
  totalSessions: { type: Number, required: true }, // always 12 (max per quarter)
  quarter: { type: String, required: true }, // "Q1 2025"
  // "auto"    = applied immediately (no approval needed)
  // "pending" = needs group leader approval (Consistent→Intense, Intense→Leader,
  //             Leader→Intense, Intense→Consistent)
  status: {
    type: String,
    enum: ["auto", "pending", "approved", "rejected"],
    default: "auto",
  },
  reviewedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  reviewedByName: String,
  reviewedAt: Date,
  rejectionNote: String,
  appliedAt: Date,
  createdAt: { type: Date, default: Date.now },
});
const StatusUpdate = mongoose.model("StatusUpdate", StatusUpdateSchema);

// ── Status Change Log — permanent audit trail of every status change ──────────
// Written on: auto-engine runs, GL approve/reject, manual PUT /members/:id edits
const StatusChangeLogSchema = new mongoose.Schema({
  memberId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Member",
    required: true,
  },
  memberName: { type: String, required: true },
  memberPhone: String,
  group: String,
  branch: { type: String, default: "MOR Head Quarter" },
  fromStatus: { type: String, required: true },
  toStatus: { type: String, required: true },
  direction: {
    type: String,
    enum: ["up", "down", "manual", "none"],
    required: true,
  },
  attendancePct: Number,
  attended: Number,
  totalSessions: Number,
  quarter: { type: String, required: true },
  changeType: {
    type: String,
    enum: ["auto", "approved", "manual"],
    required: true,
  },
  changedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  changedByName: String,
  statusUpdateRef: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "StatusUpdate",
  },
  loggedAt: { type: Date, default: Date.now },
});
StatusChangeLogSchema.index({ quarter: 1, group: 1 });
StatusChangeLogSchema.index({ memberId: 1, quarter: 1 });
const StatusChangeLog = mongoose.model(
  "StatusChangeLog",
  StatusChangeLogSchema,
);

// Helper: write one log entry (fire-and-forget safe)
async function writeStatusLog({
  memberId,
  memberName,
  memberPhone,
  group,
  branch,
  fromStatus,
  toStatus,
  direction,
  attendancePct,
  attended,
  totalSessions,
  quarter,
  changeType,
  changedBy,
  changedByName,
  statusUpdateRef,
}) {
  try {
    await StatusChangeLog.create({
      memberId,
      memberName,
      memberPhone,
      group,
      branch: branch || "MOR Head Quarter",
      fromStatus,
      toStatus,
      direction,
      attendancePct,
      attended,
      totalSessions,
      quarter,
      changeType,
      changedBy: changedBy || null,
      changedByName: changedByName || null,
      statusUpdateRef: statusUpdateRef || null,
    });
  } catch (e) {
    console.error("StatusChangeLog write error:", e.message);
  }
}

// Helper: get current quarter label
function currentQuarterLabel(d = new Date()) {
  const q = Math.floor(d.getMonth() / 3) + 1;
  return `Q${q} ${d.getFullYear()}`;
}

// ========== AUTH MIDDLEWARE ==========
const authMiddleware = async (req, res, next) => {
  try {
    const token = req.header("Authorization")?.replace("Bearer ", "");
    if (!token) return res.status(401).json({ error: "Please authenticate" });
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    const user = await User.findById(decoded.userId).select("-password");
    if (!user) return res.status(401).json({ error: "User not found" });
    req.user = user;
    next();
  } catch (error) {
    res.status(401).json({ error: "Invalid token" });
  }
};

const roleMiddleware =
  (...roles) =>
  (req, res, next) => {
    if (!roles.includes(req.user.role))
      return res.status(403).json({ error: "Access denied" });
    next();
  };

// ========== MULTER CONFIGURATION ==========
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (/jpeg|jpg|png|gif|webp/.test(file.mimetype)) return cb(null, true);
    cb(new Error("Only image files are allowed"));
  },
});
const mediaUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 100 * 1024 * 1024 },
});
const excelUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok =
      file.mimetype ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      file.mimetype === "application/vnd.ms-excel" ||
      file.originalname.endsWith(".xlsx") ||
      file.originalname.endsWith(".xls");
    if (ok) return cb(null, true);
    cb(new Error("Only Excel files (.xlsx / .xls) are allowed"));
  },
});

app.use(
  "/uploads",
  express.static(path.join(__dirname, "../frontend/uploads")),
);
app.use(
  "/uploads/media",
  express.static(path.join(__dirname, "../frontend/uploads/media")),
);

// ========== DATABASE CONNECTION ==========
console.log("🔌 Connecting to MongoDB Atlas...");
mongoose
  .connect(process.env.MONGODB_URI)
  .then(() => {
    console.log("✅ Connected to MongoDB Atlas!");
    initializeDatabase();
  })
  .catch((err) => {
    console.error("❌ MongoDB error:", err.message);
    process.exit(1);
  });

// ========== HELPERS ==========
async function logActivity(action, user, details = "") {
  try {
    await ActivityLog.create({
      action,
      user: user?._id,
      userName: user?.fullName || "System",
      userRole: user?.role || "System",
      branch: user?.branch || null,
      details,
    });
  } catch (e) {}
}

async function sendSystemNotification(
  title,
  message,
  type = "general",
  targetGroup = null,
  targetUser = null,
) {
  try {
    await Notification.create({
      title,
      message,
      type,
      targetGroup: targetGroup || undefined,
      targetUser: targetUser || undefined,
      sentByName: "MOR System",
      sentByRole: "System",
    });
  } catch (e) {}
}

// QR timing helper
const QR_CUTOFFS = {
  fellowship: { h: 15, m: 30 },
  cbs: { h: 20, m: 0 },
  evangelism: { h: 21, m: 0 },
};
function getQRExpiry(type, dateStr) {
  const d = new Date(dateStr);
  const cut = QR_CUTOFFS[type] || { h: 23, m: 59 };
  d.setHours(cut.h, cut.m, 0, 0);
  return d;
}
function isOnTime(type, scanTime) {
  const t = new Date(scanTime);
  const h = t.getHours(),
    m = t.getMinutes();
  const mins = h * 60 + m;
  if (type === "fellowship") return mins <= 13 * 60 + 45;
  if (type === "cbs") return mins <= 18 * 60 + 20;
  return true;
}

// ========== SERVE PAGES ==========
app.get("/", (req, res) =>
  res.sendFile(path.join(__dirname, "../frontend/index.html")),
);
app.get("/group-leader", (req, res) =>
  res.sendFile(path.join(__dirname, "../frontend/group-leader.html")),
);
app.get("/member", (req, res) =>
  res.sendFile(path.join(__dirname, "../frontend/member.html")),
);
app.get("/qr-scan", (req, res) =>
  res.sendFile(path.join(__dirname, "../frontend/qr-scan.html")),
);
app.get("/branch", (req, res) =>
  res.sendFile(path.join(__dirname, "../frontend/branch.html")),
);
app.get("/branch.html", (req, res) =>
  res.sendFile(path.join(__dirname, "../frontend/branch.html")),
);
app.get("/api", (req, res) =>
  res.json({ name: "MOR System API", version: "2.0.0", status: "running" }),
);
app.get("/api/health", (req, res) =>
  res.json({
    status: "OK",
    timestamp: new Date(),
    database:
      mongoose.connection.readyState === 1 ? "Connected" : "Disconnected",
  }),
);

// ========== AUTH ROUTES ==========
app.post(
  "/api/auth/register-member",
  authMiddleware,
  roleMiddleware(
    "Head Shepherd",
    "Branch Head Shepherd",
    "Group Leader",
    "System Admin",
  ),
  async (req, res) => {
    try {
      const {
        fullName,
        phoneNumber,
        password,
        group,
        membershipStatus,
        branch,
        role,
        ...otherFields
      } = req.body;
      if (!fullName || !phoneNumber || !password)
        return res
          .status(400)
          .json({ error: "fullName, phoneNumber and password are required" });
      if (await User.findOne({ phoneNumber }))
        return res
          .status(400)
          .json({ error: "Phone number already registered" });
      const hashedPassword = await bcrypt.hash(password, 10);
      const userBranch = branch || req.user.branch || "MOR Head Quarter";

      // Determine user role from member role field
      let userRole = "Member";
      if (role === "Head Shepherd") userRole = "Head Shepherd";
      else if (role === "Branch Head Shepherd")
        userRole = "Branch Head Shepherd";
      else if (role === "Group Leader") userRole = "Group Leader";

      const user = new User({
        fullName,
        phoneNumber,
        password: hashedPassword,
        role: userRole,
        branch: userBranch,
        group: group || null,
        membershipStatus: membershipStatus || "First Timer",
        ...otherFields,
      });
      await user.save();
      let member = await Member.findOne({ phoneNumber });
      if (!member) {
        member = new Member({
          fullName,
          phoneNumber,
          branch: userBranch,
          group: group || null,
          role: role || "Member",
          membershipStatus: membershipStatus || "First Timer",
          ...otherFields,
          addedBy: req.user._id,
          addedByName: req.user.fullName,
        });
        await member.save();
      }
      if (member.group)
        await Group.findOneAndUpdate(
          { name: member.group },
          { $inc: { memberCount: 1 } },
        );
      await logActivity(`registered member ${fullName} with account`, req.user);
      res
        .status(201)
        .json({ message: "Member registered successfully", member });
    } catch (error) {
      res.status(500).json({ error: error.message || "Server error" });
    }
  },
);

app.post("/api/auth/register", async (req, res) => {
  try {
    const { fullName, phoneNumber, password, group, branch, ...otherFields } =
      req.body;
    if (await User.findOne({ phoneNumber }))
      return res.status(400).json({ error: "Phone number already registered" });
    const userCount = await User.countDocuments();
    let role = "Member";
    if (userCount === 0) role = "Head Shepherd";
    const hashedPassword = await bcrypt.hash(password, 10);
    const user = new User({
      fullName,
      phoneNumber,
      password: hashedPassword,
      role,
      branch: branch || "MOR Head Quarter",
      group: group || null,
      ...otherFields,
    });
    await user.save();
    const member = new Member({
      fullName,
      phoneNumber,
      branch: branch || "MOR Head Quarter",
      group: group || null,
      membershipStatus: "First Timer",
      ...otherFields,
      addedBy: user._id,
      addedByName: user.fullName,
    });
    await member.save();
    const token = jwt.sign(
      { userId: user._id, role: user.role },
      process.env.JWT_SECRET,
      { expiresIn: "7d" },
    );
    res.status(201).json({
      message: "Registration successful",
      token,
      user: {
        id: user._id,
        fullName: user.fullName,
        phoneNumber: user.phoneNumber,
        role: user.role,
        group: user.group,
        branch: user.branch,
      },
    });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const { phoneNumber, password } = req.body;
    const user = await User.findOne({ phoneNumber });
    if (!user) return res.status(401).json({ error: "Invalid credentials" });
    if (!(await bcrypt.compare(password, user.password)))
      return res.status(401).json({ error: "Invalid credentials" });
    const token = jwt.sign(
      { userId: user._id, role: user.role },
      process.env.JWT_SECRET,
      { expiresIn: "7d" },
    );
    user.lastLogin = new Date();
    await user.save();
    await logActivity("logged in", user);
    res.json({
      message: "Login successful",
      token,
      user: {
        id: user._id,
        fullName: user.fullName,
        phoneNumber: user.phoneNumber,
        role: user.role,
        group: user.group,
        branch: user.branch,
        isSteward: user.isSteward,
        membershipStatus: user.membershipStatus,
        isBranchShepherd: user.isBranchShepherd,
        isCBSLeader: user.isCBSLeader,
        assignedCBSLocation: user.assignedCBSLocation,
        isGroupLeader: user.isGroupLeader,
      },
    });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.get("/api/auth/verify", authMiddleware, async (req, res) => {
  res.json({
    valid: true,
    user: {
      id: req.user._id,
      role: req.user.role,
      fullName: req.user.fullName,
      branch: req.user.branch,
    },
  });
});

// ========== PROFILE ROUTES ==========
app.post(
  "/api/profile/photo",
  authMiddleware,
  upload.single("photo"),
  async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: "No file uploaded" });
      const photoUrl = await new Promise((resolve, reject) => {
        const stream = cloudinary.uploader.upload_stream(
          {
            folder: "mor-system/profile-photos",
            transformation: [
              { width: 500, height: 500, crop: "fill", gravity: "face" },
            ],
          },
          (error, result) => {
            if (error) return reject(error);
            resolve(result.secure_url);
          },
        );
        stream.end(req.file.buffer);
      });
      await User.findByIdAndUpdate(req.user._id, { profilePhoto: photoUrl });
      await Member.findOneAndUpdate(
        { phoneNumber: req.user.phoneNumber },
        { profilePhoto: photoUrl },
      );
      res.json({ photoUrl, message: "Profile photo updated successfully" });
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.get("/api/profile", authMiddleware, async (req, res) => {
  try {
    const user = await User.findById(req.user._id).select("-password");
    const member = await Member.findOne({ phoneNumber: req.user.phoneNumber });
    const profile = {
      ...(member ? member.toObject() : {}),
      fullName: user.fullName,
      phoneNumber: user.phoneNumber,
      role: user.role,
      branch: user.branch || member?.branch || "MOR Head Quarter",
      profilePhoto: member?.profilePhoto || user.profilePhoto || "",
      dateOfBirth: member?.dateOfBirth || user.dateOfBirth,
      gender: member?.gender || user.gender,
      address: member?.address || user.address,
      occupation: member?.occupation || user.occupation,
      school: member?.school || user.school,
      church: member?.church || user.church,
      cbsLocation: member?.cbsLocation || user.cbsLocation,
      group: member?.group || user.group,
      membershipStatus: member?.membershipStatus || user.membershipStatus,
      isSteward: member?.isSteward || user.isSteward || false,
      stewardSince: member?.stewardSince || user.stewardSince,
      isCBSLeader: user.isCBSLeader || false,
      assignedCBSLocation: user.assignedCBSLocation || null,
      isBranchShepherd: user.isBranchShepherd || false,
      assignedTo: member?.assignedTo || null,
      assignedToName: member?.assignedToName || null,
      userId: user._id,
    };
    res.json(profile);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.put("/api/profile", authMiddleware, async (req, res) => {
  try {
    const {
      fullName,
      address,
      occupation,
      school,
      church,
      profilePhoto,
      dateOfBirth,
      gender,
      cbsLocation,
    } = req.body;
    const user = await User.findById(req.user._id);
    if (!user) return res.status(404).json({ error: "User not found" });
    if (fullName) user.fullName = fullName;
    if (address !== undefined) user.address = address;
    if (occupation !== undefined) user.occupation = occupation;
    if (school !== undefined) user.school = school;
    if (church !== undefined) user.church = church;
    if (profilePhoto) user.profilePhoto = profilePhoto;
    if (dateOfBirth) user.dateOfBirth = new Date(dateOfBirth);
    if (gender) user.gender = gender;
    if (cbsLocation !== undefined) user.cbsLocation = cbsLocation;
    await user.save();
    const memberUpdate = { updatedAt: new Date() };
    if (fullName) memberUpdate.fullName = fullName;
    if (address !== undefined) memberUpdate.address = address;
    if (occupation !== undefined) memberUpdate.occupation = occupation;
    if (school !== undefined) memberUpdate.school = school;
    if (church !== undefined) memberUpdate.church = church;
    if (profilePhoto) memberUpdate.profilePhoto = profilePhoto;
    if (dateOfBirth) memberUpdate.dateOfBirth = new Date(dateOfBirth);
    if (gender) memberUpdate.gender = gender;
    if (cbsLocation !== undefined) memberUpdate.cbsLocation = cbsLocation;
    await Member.findOneAndUpdate(
      { phoneNumber: user.phoneNumber },
      memberUpdate,
      { new: true },
    );
    await logActivity("updated their profile", req.user);
    res.json({ ...user.toObject(), password: undefined });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.put("/api/profile/password", authMiddleware, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;
    const user = await User.findById(req.user._id);
    if (!(await bcrypt.compare(currentPassword, user.password)))
      return res.status(401).json({ error: "Current password is incorrect" });
    user.password = await bcrypt.hash(newPassword, 10);
    await user.save();
    res.json({ message: "Password updated successfully" });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== MEMBERS ROUTES ==========
app.get("/api/members", authMiddleware, async (req, res) => {
  try {
    const { group, status, search, steward, cbsLocation, branch, assignedTo } =
      req.query;
    let query = {};
    // cbsOnly=true allows bypassing the group filter ONLY for CBS Location Leaders,
    // so they can see all members across groups at their assigned CBS location.
    // Regular Group Leaders always stay scoped to their own group.
    const cbsOnly =
      req.query.cbsOnly === "true" && req.user.isCBSLeader === true;
    if (!cbsOnly && req.user.role === "Group Leader" && req.user.group)
      query.group = req.user.group;
    if (req.user.role === "Branch Head Shepherd" && req.user.branch)
      query.branch = req.user.branch;
    if (group && group !== "All Groups") query.group = group;
    if (branch && branch !== "All Branches") query.branch = branch;
    if (status && status !== "All Statuses") query.membershipStatus = status;
    if (steward === "true") query.isSteward = true;
    if (cbsLocation && cbsLocation !== "All Locations")
      query.cbsLocation = cbsLocation;
    if (assignedTo) query.assignedTo = assignedTo;
    if (search)
      query.$or = [
        { fullName: { $regex: search, $options: "i" } },
        { phoneNumber: { $regex: search, $options: "i" } },
      ];
    const members = await Member.find(query).sort({ createdAt: -1 });
    const phoneNumbers = members.map((m) => m.phoneNumber);
    const users = await User.find(
      { phoneNumber: { $in: phoneNumbers } },
      { phoneNumber: 1, profilePhoto: 1 },
    );
    const userPhotoMap = {};
    users.forEach((u) => {
      if (u.profilePhoto) userPhotoMap[u.phoneNumber] = u.profilePhoto;
    });
    const enriched = members.map((m) => {
      const obj = m.toObject();
      if (!obj.profilePhoto && userPhotoMap[m.phoneNumber])
        obj.profilePhoto = userPhotoMap[m.phoneNumber];
      return obj;
    });
    res.json(enriched);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/members", authMiddleware, async (req, res) => {
  try {
    if (req.user.role === "Group Leader" && req.body.group !== req.user.group)
      return res
        .status(403)
        .json({ error: "You can only add members to your own group" });
    const member = new Member({
      ...req.body,
      branch: req.body.branch || req.user.branch || "MOR Head Quarter",
      addedBy: req.user._id,
      addedByName: req.user.fullName,
    });
    await member.save();
    if (member.group)
      await Group.findOneAndUpdate(
        { name: member.group },
        { $inc: { memberCount: 1 } },
      );
    res.status(201).json(member);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.put("/api/members/:id", authMiddleware, async (req, res) => {
  try {
    const member = await Member.findById(req.params.id);
    if (!member) return res.status(404).json({ error: "Member not found" });
    if (req.user.role === "Group Leader" && member.group !== req.user.group)
      return res
        .status(403)
        .json({ error: "You can only update members in your own group" });

    // ── FIX #3: Handle role field update — also sync User table role ──
    const updateData = { ...req.body, updatedAt: new Date() };
    const updatedMember = await Member.findByIdAndUpdate(
      req.params.id,
      updateData,
      { new: true, runValidators: false },
    );

    // Sync role to User table if role changed
    if (req.body.role) {
      const roleMap = {
        "Head Shepherd": "Head Shepherd",
        "Branch Head Shepherd": "Branch Head Shepherd",
        "Group Leader": "Group Leader",
        Member: "Member",
      };
      const newRole = roleMap[req.body.role] || "Member";
      await User.findOneAndUpdate(
        { phoneNumber: member.phoneNumber },
        {
          role: newRole,
          isBranchShepherd: newRole === "Branch Head Shepherd",
          isGroupLeader: newRole === "Group Leader",
        },
        { runValidators: false },
      );
    }
    if (req.body.profilePhoto)
      await User.findOneAndUpdate(
        { phoneNumber: member.phoneNumber },
        { profilePhoto: req.body.profilePhoto },
        { runValidators: false },
      );
    // ── If membershipStatus changed, write a StatusChangeLog entry ──
    if (
      req.body.membershipStatus &&
      req.body.membershipStatus !== member.membershipStatus
    ) {
      const oldStatus = member.membershipStatus;
      const newStatus = req.body.membershipStatus;
      const statusOrder = [
        "First Timer",
        "Inconsistent",
        "Semi-Consistent",
        "Consistent",
        "Intense Leader",
        "Leader",
      ];
      const oldIdx = statusOrder.indexOf(oldStatus);
      const newIdx = statusOrder.indexOf(newStatus);
      const direction =
        newIdx > oldIdx ? "up" : newIdx < oldIdx ? "down" : "manual";
      await writeStatusLog({
        memberId: member._id,
        memberName: member.fullName,
        memberPhone: member.phoneNumber,
        group: member.group,
        branch: member.branch || "MOR Head Quarter",
        fromStatus: oldStatus,
        toStatus: newStatus,
        direction,
        attendancePct: null,
        attended: null,
        totalSessions: null,
        quarter: currentQuarterLabel(),
        changeType: "manual",
        changedBy: req.user._id,
        changedByName: req.user.fullName,
      });
    }

    await logActivity(`updated member ${member.fullName}`, req.user);
    res.json(updatedMember);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ── BULK IMPORT: POST /api/members/import-excel ──────────────────────────────
// Accepts an .xlsx/.xls file, parses columns:
//   Full Name | Phone | Membership Status | Group | Gender | CBS Location (optional)
// Creates both Member + User records (password = "member123") for each row.
// Skips rows where the phone number already exists. Logs the upload.
app.post(
  "/api/members/import-excel",
  authMiddleware,
  roleMiddleware(
    "Head Shepherd",
    "Branch Head Shepherd",
    "Group Leader",
    "System Admin",
  ),
  excelUpload.single("excelFile"),
  async (req, res) => {
    try {
      if (!req.file)
        return res.status(400).json({ error: "No Excel file uploaded" });

      // Dynamically require xlsx so the rest of the server still works if xlsx not installed
      let XLSX;
      try {
        XLSX = require("xlsx");
      } catch (e) {
        return res.status(500).json({
          error: "xlsx package not installed on server. Run: npm install xlsx",
        });
      }

      const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];

      // ── Smart parser: handles any column naming + section-label statuses ──
      const normHdr = (s) =>
        String(s || "")
          .trim()
          .toLowerCase()
          .replace(/[\s_-]+/g, " ");

      // All accepted aliases for each field
      const FIELD_ALIASES = {
        fullName: [
          "full name",
          "fullname",
          "name",
          "member",
          "members",
          "member name",
          "participant",
          "participants",
        ],
        phone: [
          "phone",
          "phone number",
          "phonenumber",
          "mobile",
          "mobile number",
          "contact",
          "contacts",
          "contact number",
          "tel",
          "telephone",
          "phone no",
          "phone#",
          "cell",
        ],
        membershipStatus: [
          "membership status",
          "status",
          "membership status",
          "memberstatus",
          "member status",
          "level",
          "category",
        ],
        group: ["group", "group name", "groupname", "cell", "cell group"],
        gender: ["gender", "sex"],
        cbsLocation: [
          "cbs location",
          "cbslocation",
          "cbs",
          "cbs location",
          "location",
          "cbs loc",
        ],
      };

      // Map Excel section/category labels → system membership status values
      const STATUS_SECTION_MAP = {
        leaders: "Leader",
        leader: "Leader",
        intense: "Intense Leader",
        "intense leader": "Intense Leader",
        consistent: "Consistent",
        "semi-consistent": "Semi-Consistent",
        "semi consistent": "Semi-Consistent",
        semiconsistent: "Semi-Consistent",
        inconsistent: "Inconsistent",
        "first timer": "First Timer",
        "first timers": "First Timer",
      };

      const VALID_STATUSES = [
        "First Timer",
        "Inconsistent",
        "Semi-Consistent",
        "Consistent",
        "Intense Leader",
        "Leader",
      ];

      // Use sheet_to_json with array-of-arrays to support section-label rows
      const rawAoA = XLSX.utils.sheet_to_json(sheet, {
        header: 1,
        defval: null,
        raw: false,
      });

      // Find header row (first row with a recognised field alias)
      const allAliases = Object.values(FIELD_ALIASES).flat();
      let headerRowIdx = -1;
      for (let i = 0; i < Math.min(rawAoA.length, 15); i++) {
        const rowNorm = (rawAoA[i] || []).map((c) => normHdr(String(c || "")));
        if (rowNorm.some((v) => allAliases.includes(v))) {
          headerRowIdx = i;
          break;
        }
      }

      // Build column index → field mapping from header row
      const colIdxMap = {};
      if (headerRowIdx >= 0) {
        (rawAoA[headerRowIdx] || []).forEach((cell, ci) => {
          const norm = normHdr(String(cell || ""));
          for (const [field, aliases] of Object.entries(FIELD_ALIASES)) {
            if (
              aliases.includes(norm) &&
              !Object.values(colIdxMap).includes(field)
            ) {
              colIdxMap[ci] = field;
            }
          }
        });
      }

      // Walk data rows, tracking running status from section-label rows
      const parsedRows = [];
      let currentSectionStatus = "";

      const startRow = headerRowIdx >= 0 ? headerRowIdx + 1 : 0;
      for (let ri = startRow; ri < rawAoA.length; ri++) {
        const row = rawAoA[ri] || [];
        const rec = {};
        for (const [ci, field] of Object.entries(colIdxMap)) {
          const val = row[ci];
          if (val !== null && val !== undefined && String(val).trim()) {
            rec[field] = String(val).trim();
          }
        }

        const nameVal = rec.fullName || "";
        // Skip formula/empty rows
        if (!nameVal || nameVal.startsWith("=")) continue;

        // Detect category label rows (name is a status keyword, no other data)
        const normName = normHdr(nameVal);
        const isSectionLabel = STATUS_SECTION_MAP[normName] !== undefined;
        const hasOtherData = Object.keys(rec).some(
          (k) => k !== "fullName" && rec[k],
        );

        if (isSectionLabel && !hasOtherData) {
          currentSectionStatus = STATUS_SECTION_MAP[normName];
          continue;
        }
        if (
          nameVal === "Members" ||
          nameVal === "Leaders" ||
          nameVal === "Intense" ||
          nameVal === "Consistent" ||
          nameVal === "Semi-Consistent" ||
          nameVal === "Inconsistent" ||
          nameVal === "First Timer"
        ) {
          // header/label row — skip
          continue;
        }

        // Resolve membership status
        let resolvedStatus = "";
        if (rec.membershipStatus) {
          const mapped = STATUS_SECTION_MAP[normHdr(rec.membershipStatus)];
          resolvedStatus =
            mapped ||
            (VALID_STATUSES.includes(rec.membershipStatus)
              ? rec.membershipStatus
              : "");
        }
        if (!resolvedStatus)
          resolvedStatus = currentSectionStatus || "First Timer";

        parsedRows.push({
          fullName: nameVal,
          phone: rec.phone || "",
          membershipStatus: resolvedStatus,
          group: rec.group || "",
          gender: rec.gender || "",
          cbsLocation: rec.cbsLocation || "",
        });
      }

      // Fallback: if smart parser got nothing, try original sheet_to_json
      if (!parsedRows.length) {
        const fallbackRows = XLSX.utils.sheet_to_json(sheet, { defval: "" });
        fallbackRows.forEach((r) => {
          const keys = Object.keys(r);
          const get = (...aliases) => {
            for (const a of aliases) {
              const k = keys.find((k) => normHdr(k) === normHdr(a));
              if (k && String(r[k]).trim()) return String(r[k]).trim();
            }
            return "";
          };
          const fn = get("full name", "fullname", "name", "member", "members");
          if (fn)
            parsedRows.push({
              fullName: fn,
              phone: (() => {
                const raw = get(
                  "phone",
                  "phone number",
                  "contact",
                  "contacts",
                  "mobile",
                  "tel",
                );
                const p = raw.replace(/\s+/g, "");
                return /^\d+$/.test(p) && p.length > 0 && p[0] !== "0"
                  ? "0" + p
                  : p;
              })(),
              membershipStatus:
                get("membership status", "status", "level") || "First Timer",
              group: get("group", "group name") || "",
              gender: get("gender", "sex") || "",
              cbsLocation: get("cbs location", "cbslocation", "cbs") || "",
            });
        });
      }

      if (!parsedRows.length)
        return res
          .status(400)
          .json({ error: "Excel file is empty or has no data rows" });

      const DEFAULT_PASSWORD = "member123";
      const hashedPassword = await bcrypt.hash(DEFAULT_PASSWORD, 10);

      const leaderGroup = req.user.group;
      const leaderBranch = req.user.branch || "MOR Head Quarter";

      let successCount = 0;
      const skippedDetails = [];

      for (const row of parsedRows) {
        const fullName = row.fullName;
        const phone = row.phone;
        const rawStatus = row.membershipStatus;
        const group = row.group || leaderGroup;
        const gender = row.gender;
        const cbsLocation = row.cbsLocation;

        if (!fullName) {
          skippedDetails.push({
            phone: phone || "—",
            name: "—",
            reason: "Missing Full Name",
          });
          continue;
        }
        // Phone missing — still import but note it
        if (!phone) {
          skippedDetails.push({
            phone: "—",
            name: fullName,
            reason: "Missing Phone — skipped",
          });
          continue;
        }

        // Clean phone: strip spaces, then restore leading zero Excel may have stripped.
        // e.g. "33230039" (8 digits, no leading 0) → "033230039"
        let cleanPhone = phone.replace(/\s+/g, "");
        if (
          /^\d+$/.test(cleanPhone) &&
          cleanPhone.length > 0 &&
          cleanPhone[0] !== "0"
        ) {
          cleanPhone = "0" + cleanPhone;
        }

        // Skip if phone already registered in User or Member
        const existingUser = await User.findOne({ phoneNumber: cleanPhone });
        const existingMember = await Member.findOne({
          phoneNumber: cleanPhone,
        });
        if (existingUser || existingMember) {
          skippedDetails.push({
            phone: cleanPhone,
            name: fullName,
            reason: "Phone already registered",
          });
          continue;
        }

        // Validate / default membership status
        const membershipStatus = VALID_STATUSES.includes(rawStatus)
          ? rawStatus
          : "First Timer";

        // Scope group: if GL, always use their own group
        const assignedGroup =
          req.user.role === "Group Leader" ? leaderGroup : group || leaderGroup;

        // Create User account (so the member can log in)
        const newUser = new User({
          fullName,
          phoneNumber: cleanPhone,
          password: hashedPassword,
          role: "Member",
          branch: leaderBranch,
          group: assignedGroup || null,
          gender: gender || undefined,
          cbsLocation: cbsLocation || undefined,
          membershipStatus,
        });
        await newUser.save();

        // Create Member record
        const newMember = new Member({
          fullName,
          phoneNumber: cleanPhone,
          branch: leaderBranch,
          group: assignedGroup || null,
          gender: gender || undefined,
          cbsLocation: cbsLocation || undefined,
          membershipStatus,
          addedBy: req.user._id,
          addedByName: req.user.fullName,
        });
        await newMember.save();

        // Increment group member count
        if (assignedGroup) {
          await Group.findOneAndUpdate(
            { name: assignedGroup },
            { $inc: { memberCount: 1 } },
          );
        }

        successCount++;
      }

      // Log the import
      await BulkImport.create({
        uploadedBy: req.user._id,
        uploadedByName: req.user.fullName,
        group: leaderGroup || "N/A",
        branch: leaderBranch,
        fileName: req.file.originalname,
        totalRows: parsedRows.length, // fixed: was `rows` (undefined)
        successCount,
        skippedCount: skippedDetails.length,
        skippedDetails,
      });

      await logActivity(
        `bulk-imported ${successCount} members from Excel (${req.file.originalname})`,
        req.user,
      );

      res.json({
        message: `Import complete. ${successCount} member(s) added, ${skippedDetails.length} skipped.`,
        successCount, // primary field
        savedCount: successCount, // alias for older clients
        skippedCount: skippedDetails.length,
        errorCount: 0, // no hard errors if we reached here
        skippedDetails, // array with name/phone/reason
        skipped: skippedDetails, // alias for older clients
      });
    } catch (error) {
      console.error("Excel import error:", error);
      res
        .status(500)
        .json({ error: error.message || "Server error during import" });
    }
  },
);

// ── ALIAS: /api/members/bulk-upload → same handler as /api/members/import-excel ──
// Kept for backwards compatibility; the canonical URL is /api/members/import-excel
app.post(
  "/api/members/bulk-upload",
  authMiddleware,
  roleMiddleware(
    "Head Shepherd",
    "Branch Head Shepherd",
    "Group Leader",
    "System Admin",
  ),
  excelUpload.single("excelFile"),
  async (req, res) => {
    // Forward internally by forwarding the request body to the import-excel logic.
    // We simply re-use the same route so both URLs work identically.
    req.url = "/api/members/import-excel";
    return app._router.handle(
      Object.assign(req, {
        url: "/api/members/import-excel",
        path: "/api/members/import-excel",
      }),
      res,
      () => res.status(404).json({ error: "Not found" }),
    );
  },
);

// ── GET /api/members/import-history — list past bulk imports for this leader ──
app.get("/api/members/import-history", authMiddleware, async (req, res) => {
  try {
    const query =
      req.user.role === "Group Leader" ? { uploadedBy: req.user._id } : {};
    const history = await BulkImport.find(query)
      .sort({ createdAt: -1 })
      .limit(20);
    res.json(history);
  } catch (e) {
    res.status(500).json({ error: "Server error" });
  }
});

app.delete(
  "/api/members/:id",
  authMiddleware,
  roleMiddleware(
    "Head Shepherd",
    "Branch Head Shepherd",
    "System Admin",
    "Group Leader",
  ),
  async (req, res) => {
    try {
      const member = await Member.findById(req.params.id);
      if (!member) return res.status(404).json({ error: "Member not found" });
      if (req.user.role === "Group Leader" && member.group !== req.user.group)
        return res
          .status(403)
          .json({ error: "You can only delete members in your own group" });
      const memberName = member.fullName;
      await Member.findByIdAndDelete(req.params.id);
      await User.findOneAndDelete({ phoneNumber: member.phoneNumber });
      await Attendance.updateMany(
        { "records.memberName": memberName },
        { $pull: { records: { memberName } } },
      );
      await Assignment.deleteMany({ member: req.params.id });
      if (member.group)
        await Group.findOneAndUpdate(
          { name: member.group },
          { $inc: { memberCount: -1 } },
        );
      await logActivity(`deleted member ${memberName}`, req.user);
      res.json({ message: `Member ${memberName} deleted successfully` });
    } catch (error) {
      res.status(500).json({ error: error.message || "Server error" });
    }
  },
);

app.post(
  "/api/members/sync-photos",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const users = await User.find(
        { profilePhoto: { $exists: true, $ne: "" } },
        { phoneNumber: 1, profilePhoto: 1 },
      );
      let synced = 0;
      for (const user of users) {
        const result = await Member.findOneAndUpdate(
          {
            phoneNumber: user.phoneNumber,
            $or: [
              { profilePhoto: { $exists: false } },
              { profilePhoto: "" },
              { profilePhoto: null },
            ],
          },
          { profilePhoto: user.profilePhoto },
          { runValidators: false },
        );
        if (result) synced++;
      }
      res.json({ message: `Synced photos for ${synced} members`, synced });
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.post(
  "/api/members/:id/toggle-steward",
  authMiddleware,
  async (req, res) => {
    try {
      const member = await Member.findById(req.params.id);
      if (!member) return res.status(404).json({ error: "Member not found" });
      if (req.user.role === "Group Leader" && member.group !== req.user.group)
        return res
          .status(403)
          .json({ error: "You can only manage stewards in your own group" });
      member.isSteward = !member.isSteward;
      member.stewardSince = member.isSteward ? new Date() : undefined;
      await member.save();
      if (member.group) {
        const stewardCount = await Member.countDocuments({
          group: member.group,
          isSteward: true,
        });
        await Group.findOneAndUpdate({ name: member.group }, { stewardCount });
      }
      res.json(member);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== ATTENDANCE ROUTES ==========

// GET /api/attendance-years — returns distinct years that have attendance records
app.get("/api/attendance-years", authMiddleware, async (req, res) => {
  try {
    const records = await Attendance.find({}, { date: 1 }).lean();
    const years = [
      ...new Set(records.map((r) => new Date(r.date).getFullYear())),
    ].sort((a, b) => b - a); // newest first
    res.json(years);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/attendance", authMiddleware, async (req, res) => {
  try {
    const { type, group, cbsLocation, date, branch } = req.query;
    let query = {};
    if (type) query.type = type;
    if (group && group !== "" && group !== "null") query.group = group;
    if (cbsLocation) query.cbsLocation = cbsLocation;
    if (branch) {
      query.branch = branch;
    } else if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      // Auto-scope attendance to Branch Head Shepherd's branch
      query.branch = req.user.branch;
    }
    if (date) {
      const start = new Date(date);
      start.setHours(0, 0, 0, 0);
      const end = new Date(start);
      end.setDate(end.getDate() + 1);
      query.date = { $gte: start, $lt: end };
    }
    if (req.user.role === "Member") {
      const member = await Member.findOne({
        phoneNumber: req.user.phoneNumber,
      });
      if (member) query["records.memberId"] = member._id;
    } else if (req.user.role === "Group Leader" && req.user.group) {
      if (!group) {
        // Include records for this group AND records saved with no group (group: null)
        // which is how "All Groups" attendance is stored when head/branch shepherd marks
        // the register for all groups at once. Use $or so both are returned.
        // Also scope to the leader's branch if available to avoid cross-branch leakage.
        const glBranchFilter = req.user.branch
          ? { branch: req.user.branch }
          : {};
        query.$or = [
          { group: req.user.group },
          { group: null, ...glBranchFilter },
        ];
      }
    } else if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      if (!branch) {
        // Find all groups in this branch so QR-created records (which always
        // have a group field set) are included even if their branch field is missing
        const branchGroupDocs = await Member.distinct("group", {
          branch: req.user.branch,
        }).catch(() => []);
        query.$or = [
          { branch: req.user.branch },
          { group: { $in: branchGroupDocs.filter(Boolean) } },
        ];
      }
    }
    const records = await Attendance.find(query).sort({ date: -1 }).lean();
    res.json(records);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/attendance", authMiddleware, async (req, res) => {
  try {
    const { type, group, cbsLocation, date, records, branch } = req.body;
    if (!type || !date || !records)
      return res.status(400).json({ error: "Missing required fields" });
    const total = records.length;
    const present = records.filter((r) => r.status === "present").length;
    const attendance = new Attendance({
      type,
      // ── FIX #5: Don't store "all" as group name — use null instead ──
      group:
        group && group !== "all" && group !== "All" && group !== ""
          ? group
          : null,
      cbsLocation: type === "cbs" ? cbsLocation : undefined,
      branch: branch || req.user.branch || null,
      date: new Date(date),
      records: records.map((r) => ({
        ...r,
        checkInTime: r.checkInTime ? new Date(r.checkInTime) : new Date(),
      })),
      stats: {
        total,
        present,
        absent: total - present,
        percentage: total > 0 ? Math.round((present / total) * 100) : 0,
      },
      recordedBy: req.user._id,
      recordedByName: req.user.fullName,
    });
    await attendance.save();
    await logActivity(`recorded ${type} attendance`, req.user);
    res.status(201).json(attendance);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.put("/api/attendance/:id", authMiddleware, async (req, res) => {
  try {
    const { records } = req.body;
    const attendance = await Attendance.findById(req.params.id);
    if (!attendance)
      return res.status(404).json({ error: "Attendance record not found" });
    attendance.records = records;
    const total = records.length,
      present = records.filter((r) => r.status === "present").length;
    attendance.stats = {
      total,
      present,
      absent: total - present,
      percentage: total > 0 ? ((present / total) * 100).toFixed(1) : 0,
    };
    await attendance.save();
    res.json(attendance);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== DASHBOARD ROUTES — FIX #1: Branch filtering for Head Shepherd ==========
app.get("/api/dashboard/stats", authMiddleware, async (req, res) => {
  try {
    let memberQuery = {},
      stewardQuery = { isSteward: true },
      groupQuery = {};

    if (req.user.role === "Group Leader" && req.user.group) {
      memberQuery.group = req.user.group;
      stewardQuery.group = req.user.group;
      groupQuery.name = req.user.group;
    } else if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      memberQuery.branch = req.user.branch;
      stewardQuery.branch = req.user.branch;
      groupQuery.branch = req.user.branch;
    } else if (req.user.role === "Member") {
      return res.json({
        totalMembers: 1,
        activeGroups: 0,
        totalStewards: 0,
        totalCBSLocations: 0,
        groupPerformance: [],
      });
    } else {
      // Head Shepherd or System Admin — respect the branch query param
      if (req.query.branch && req.query.branch !== "all") {
        memberQuery.branch = req.query.branch;
        stewardQuery.branch = req.query.branch;
        groupQuery.branch = req.query.branch;
      }
    }

    const [totalMembers, totalStewards, groups, totalCBSLocations] =
      await Promise.all([
        Member.countDocuments(memberQuery),
        Member.countDocuments(stewardQuery),
        Group.find(groupQuery),
        CBSLocation.countDocuments(
          req.user.role === "Branch Head Shepherd" && req.user.branch
            ? { branch: req.user.branch }
            : groupQuery.branch
              ? { branch: groupQuery.branch }
              : {},
        ),
      ]);

    const groupPerformance = [];
    for (const group of groups) {
      const gm = await Member.find({ group: group.name });
      groupPerformance.push({
        name: group.name,
        memberCount: gm.length,
        stewardCount: gm.filter((m) => m.isSteward).length,
        intenseLeaderCount: gm.filter(
          (m) => m.membershipStatus === "Intense Leader",
        ).length,
      });
    }

    res.json({
      totalMembers,
      activeGroups: groups.length,
      totalStewards,
      totalCBSLocations,
      groupPerformance,
    });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== GROUPS ROUTES ==========
// Optional auth middleware — reads token if present, allows public access if not
const optionalAuth = async (req, res, next) => {
  try {
    const token = req.header("Authorization")?.replace("Bearer ", "");
    if (token) {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      req.user = await User.findById(decoded.userId).select("-password");
    }
  } catch (_) {}
  next();
};

app.get("/api/groups", optionalAuth, async (req, res) => {
  try {
    const query = {};
    if (req.query.branch) {
      // Explicit branch param takes priority (used by signup and index.html filters)
      query.branch = req.query.branch;
    } else if (req.user && req.user.role === "Branch Head Shepherd") {
      // Authenticated Branch Head Shepherd with no explicit param — scope to their branch
      query.branch = req.user.branch;
    }
    const groups = await Group.find(query);
    const memberQuery = query.branch ? { branch: query.branch } : {};
    const members = await Member.find(memberQuery);
    const groupsWithCounts = groups.map((group) => ({
      ...group.toObject(),
      memberCount: members.filter((m) => m.group === group.name).length,
      stewardCount: members.filter((m) => m.group === group.name && m.isSteward)
        .length,
      intenseLeaderCount: members.filter(
        (m) =>
          m.group === group.name && m.membershipStatus === "Intense Leader",
      ).length,
      consistentCount: members.filter(
        (m) => m.group === group.name && m.membershipStatus === "Consistent",
      ).length,
    }));
    res.json(groupsWithCounts);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post(
  "/api/groups",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Branch Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { name, description, leaderId, branch } = req.body;
      if (!name)
        return res.status(400).json({ error: "Group name is required" });
      if (await Group.findOne({ name }))
        return res.status(400).json({ error: "Group already exists" });
      let leaderInfo = {};
      if (leaderId) {
        let leaderUser =
          (await User.findById(leaderId)) ||
          (await User.findOne({
            phoneNumber: (await Member.findById(leaderId))?.phoneNumber,
          }));
        if (leaderUser) {
          leaderInfo = {
            leader: leaderUser._id,
            leaderName: leaderUser.fullName,
            leaderPhone: leaderUser.phoneNumber,
          };
          await User.findByIdAndUpdate(leaderUser._id, {
            isGroupLeader: true,
            role: "Group Leader",
          });
        }
      }
      const group = new Group({
        name,
        branch: branch || req.user.branch || "MOR Head Quarter",
        description: description || `${name} Ministry Group`,
        isActive: true,
        memberCount: 0,
        ...leaderInfo,
      });
      await group.save();
      await logActivity(`created group "${name}"`, req.user);
      res.status(201).json(group);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.put(
  "/api/groups/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Branch Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { name, description, leaderId, assistantLeaderId } = req.body;
      const group = await Group.findById(req.params.id);
      if (!group) return res.status(404).json({ error: "Group not found" });
      // Branch Head Shepherd can only update groups in their own branch
      if (
        req.user.role === "Branch Head Shepherd" &&
        group.branch !== req.user.branch
      )
        return res
          .status(403)
          .json({ error: "You can only update groups in your own branch" });
      if (name) group.name = name;
      if (description !== undefined) group.description = description;
      const resolveToUser = async (id) => {
        if (!id) return null;
        return (
          (await User.findById(id)) ||
          (await User.findOne({
            phoneNumber: (await Member.findById(id))?.phoneNumber,
          }))
        );
      };
      if (leaderId !== undefined) {
        const u = await resolveToUser(leaderId);
        if (u) {
          group.leader = u._id;
          group.leaderName = u.fullName;
          group.leaderPhone = u.phoneNumber;
          await User.findByIdAndUpdate(u._id, {
            isGroupLeader: true,
            role: "Group Leader",
            group: group.name,
          });
          await Member.findOneAndUpdate(
            { phoneNumber: u.phoneNumber },
            { isGroupLeader: true, group: group.name },
            { runValidators: false },
          );
        } else if (!leaderId) {
          group.leader = null;
          group.leaderName = null;
          group.leaderPhone = null;
        }
      }
      if (assistantLeaderId !== undefined) {
        const u = await resolveToUser(assistantLeaderId);
        if (u) {
          group.assistantLeader = u._id;
          group.assistantLeaderName = u.fullName;
          group.assistantLeaderPhone = u.phoneNumber;
          await User.findByIdAndUpdate(u._id, {
            isGroupLeader: true,
            role: "Group Leader",
            group: group.name,
          });
        } else if (!assistantLeaderId) {
          group.assistantLeader = null;
          group.assistantLeaderName = null;
          group.assistantLeaderPhone = null;
        }
      }
      await group.save();
      res.json(group);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.delete(
  "/api/groups/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Branch Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const group = await Group.findById(req.params.id);
      if (!group) return res.status(404).json({ error: "Group not found" });
      // Branch Head Shepherd can only delete groups in their own branch
      if (
        req.user.role === "Branch Head Shepherd" &&
        group.branch !== req.user.branch
      )
        return res
          .status(403)
          .json({ error: "You can only delete groups in your own branch" });
      await Group.findByIdAndDelete(req.params.id);
      res.json({ message: "Group deleted successfully" });
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== CBS LOCATIONS ==========
app.get("/api/cbs-locations", async (req, res) => {
  try {
    const query = {};
    if (req.query.branch) query.branch = req.query.branch;
    else if (req.user?.role === "Branch Head Shepherd")
      query.branch = req.user.branch;
    res.json(await CBSLocation.find(query));
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post(
  "/api/cbs-locations",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Branch Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { name, leaderId, associatedGroups, branch } = req.body;
      if (await CBSLocation.findOne({ name }))
        return res.status(400).json({ error: "CBS location already exists" });
      let leaderInfo = {};
      if (leaderId) {
        const leader = await User.findById(leaderId);
        if (leader)
          leaderInfo = {
            leader: leaderId,
            leaderName: leader.fullName,
            leaderPhone: leader.phoneNumber,
          };
      }
      const location = new CBSLocation({
        name,
        branch: branch || req.user.branch || "MOR Head Quarter",
        associatedGroups: associatedGroups || [],
        status: "Active",
        ...leaderInfo,
      });
      await location.save();
      res.status(201).json(location);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.put("/api/cbs-locations/:id", authMiddleware, async (req, res) => {
  try {
    const location = await CBSLocation.findById(req.params.id);
    if (!location)
      return res.status(404).json({ error: "CBS location not found" });
    const { name, leaderId, status } = req.body;
    if (name) location.name = name;
    if (status) location.status = status;
    if (leaderId) {
      let leaderUser = await User.findById(leaderId);
      if (!leaderUser) {
        const lm = await Member.findById(leaderId);
        if (lm)
          leaderUser = await User.findOne({ phoneNumber: lm.phoneNumber });
      }
      if (leaderUser) {
        location.leader = leaderUser._id;
        location.leaderName = leaderUser.fullName;
        location.leaderPhone = leaderUser.phoneNumber;
        await User.findByIdAndUpdate(leaderUser._id, {
          isCBSLeader: true,
          assignedCBSLocation: location.name,
        });
        await Member.findOneAndUpdate(
          { phoneNumber: leaderUser.phoneNumber },
          { isCBSLeader: true, assignedCBSLocation: location.name },
        );
      }
    } else if (leaderId === "" || leaderId === null) {
      location.leader = null;
      location.leaderName = null;
      location.leaderPhone = null;
    }
    await location.save();
    res.json(location);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.delete(
  "/api/cbs-locations/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const loc = await CBSLocation.findByIdAndDelete(req.params.id);
      if (!loc)
        return res.status(404).json({ error: "CBS location not found" });
      res.json({ message: "CBS location deleted successfully" });
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== BRANCHES ==========
app.get("/api/branches", async (req, res) => {
  try {
    const branches = await Branch.find();
    res.json(branches);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post(
  "/api/branches",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { name, description } = req.body;
      if (!name)
        return res.status(400).json({ error: "Branch name is required" });
      if (await Branch.findOne({ name }))
        return res.status(400).json({ error: "Branch already exists" });
      const branch = new Branch({
        name,
        description: description || `${name} Branch`,
        isActive: true,
      });
      await branch.save();
      await logActivity(`created branch "${name}"`, req.user);
      res.status(201).json(branch);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== FIX #4: Branch Shepherd Assignment — saves name to DB + updates User table ==========
app.put(
  "/api/branches/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const branch = await Branch.findById(req.params.id);
      if (!branch) return res.status(404).json({ error: "Branch not found" });
      const { name, description, headShepherdId, headShepherdPhone } = req.body;
      if (name) branch.name = name;
      if (description) branch.description = description;
      if (headShepherdId || headShepherdPhone) {
        // Support both ID-based and phone-based lookup
        let shepherd = null;
        if (headShepherdId) {
          shepherd = await User.findById(headShepherdId).catch(() => null);
          if (!shepherd) {
            const m = await Member.findById(headShepherdId).catch(() => null);
            if (m)
              shepherd = await User.findOne({ phoneNumber: m.phoneNumber });
          }
        }
        if (!shepherd && headShepherdPhone) {
          shepherd = await User.findOne({ phoneNumber: headShepherdPhone });
          // Also try Member lookup to get the User
          if (!shepherd) {
            const m = await Member.findOne({ phoneNumber: headShepherdPhone });
            if (m)
              shepherd = await User.findOne({ phoneNumber: m.phoneNumber });
          }
        }
        if (shepherd) {
          branch.headShepherd = shepherd._id;
          branch.headShepherdName = shepherd.fullName;
          branch.headShepherdPhone = shepherd.phoneNumber;

          // Update User record with Branch Head Shepherd role
          await User.findByIdAndUpdate(shepherd._id, {
            role: "Branch Head Shepherd",
            branch: branch.name,
            isBranchShepherd: true,
          });

          // Also update Member record for consistency
          await Member.findOneAndUpdate(
            { phoneNumber: shepherd.phoneNumber },
            {
              role: "Branch Head Shepherd",
              branch: branch.name,
              isBranchShepherd: true,
            },
            { runValidators: false },
          );

          console.log(
            `✅ Assigned ${shepherd.fullName} as Branch Head Shepherd of ${branch.name}`,
          );
        }
      }
      const savedBranch = await branch.save();
      res.json(savedBranch);
    } catch (error) {
      console.error("Branch update error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.delete(
  "/api/branches/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const b = await Branch.findByIdAndDelete(req.params.id);
      if (!b) return res.status(404).json({ error: "Branch not found" });
      res.json({ message: "Branch deleted" });
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== QR ROUTES ==========
app.post(
  "/api/qr/generate",
  authMiddleware,
  roleMiddleware(
    "Head Shepherd",
    "Branch Head Shepherd",
    "Group Leader",
    "System Admin",
  ),
  async (req, res) => {
    try {
      const { type, group, cbsLocation, date } = req.body;
      if (!type || !date)
        return res.status(400).json({ error: "type and date required" });
      const sessionDate = new Date(date);
      // Allow null/empty group = "all groups" QR (Head Shepherd / Branch Shepherd)
      // If group is explicitly sent as empty string keep it null so member's own group is used at scan time
      const effectiveGroup =
        group || (req.user.role === "Group Leader" ? req.user.group : null);

      // ── Check for an existing session for the same group+type+date+branch ──
      // Scope to the user's own branch so Branch Head Shepherds never accidentally
      // reuse a session from a different branch (which would then not appear in
      // their Recent QR Sessions list because createdBy/branch won't match).
      const dateStr = sessionDate.toISOString().split("T")[0];
      const branchScope =
        req.user.role !== "Head Shepherd" && req.user.role !== "System Admin"
          ? req.user.branch || null
          : undefined; // Head Shepherd / System Admin: no branch scope on reuse lookup

      const existingQueryBase =
        type === "cbs"
          ? {
              type,
              group: effectiveGroup,
              cbsLocation,
              date: {
                $gte: new Date(dateStr + "T00:00:00.000Z"),
                $lt: new Date(dateStr + "T23:59:59.999Z"),
              },
            }
          : {
              type,
              group: effectiveGroup,
              date: {
                $gte: new Date(dateStr + "T00:00:00.000Z"),
                $lt: new Date(dateStr + "T23:59:59.999Z"),
              },
            };
      // Add branch scope for non-Head-Shepherd roles
      if (branchScope !== undefined) {
        existingQueryBase.branch = branchScope;
      }
      const existing = await QRSession.findOne(existingQueryBase);
      if (existing) {
        const qrUrl = `${process.env.FRONTEND_URL || "https://mor-system-app.vercel.app"}/qr-scan.html?token=${existing.token}`;
        return res.json({
          token: existing.token,
          qrUrl,
          session: existing,
          reused: true,
        });
      }

      // ── No existing session — create a new one ───────────────────────────
      const expiresAt = getQRExpiry(type, date);
      if (new Date() > expiresAt)
        return res.status(400).json({
          error: "Registration time has already closed for this session",
        });
      const token = crypto.randomBytes(24).toString("hex");
      const session = new QRSession({
        token,
        type,
        group: effectiveGroup,
        cbsLocation,
        branch: req.user.branch || "MOR Head Quarter",
        date: sessionDate,
        expiresAt,
        createdBy: req.user._id,
        createdByName: req.user.fullName,
        isActive: true,
      });
      await session.save();
      const qrUrl = `${process.env.FRONTEND_URL || "https://mor-system-app.vercel.app"}/qr-scan.html?token=${token}`;
      res.status(201).json({ token, qrUrl, session });
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.get("/api/qr/session/:token", async (req, res) => {
  try {
    const session = await QRSession.findOne({ token: req.params.token });
    if (!session) return res.status(404).json({ error: "Invalid QR code" });
    const now = new Date();
    if (now > session.expiresAt)
      return res.status(410).json({
        error: "QR session expired — registration is closed",
        expired: true,
      });
    res.json({
      session: {
        type: session.type,
        group: session.group,
        cbsLocation: session.cbsLocation,
        date: session.date,
        expiresAt: session.expiresAt,
        branch: session.branch,
      },
    });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/qr/scan/:token", authMiddleware, async (req, res) => {
  try {
    const session = await QRSession.findOne({
      token: req.params.token,
      isActive: true,
    });
    if (!session)
      return res.status(404).json({ error: "Invalid or inactive QR code" });
    const now = new Date();
    if (now > session.expiresAt)
      return res.status(410).json({
        error: "Attendance registration is now closed for this session",
        expired: true,
      });

    // Resolve the scanning member
    const member = await Member.findOne({ phoneNumber: req.user.phoneNumber });
    if (!member)
      return res
        .status(404)
        .json({ error: "Member record not found for your account" });

    // Already scanned guard
    const alreadyScanned = session.scans.find(
      (s) => s.memberId?.toString() === member._id.toString(),
    );
    if (alreadyScanned)
      return res.status(409).json({
        error: "You have already scanned for this session",
        alreadyScanned: true,
        memberName: member.fullName,
        group: member.group,
        membershipStatus: member.membershipStatus,
        phoneNumber: member.phoneNumber,
        timing: alreadyScanned.timing,
        scanTime: alreadyScanned.scanTime,
      });

    const timing = isOnTime(session.type, now) ? "on_time" : "late";

    // Add scan to QRSession — include member's branch so Branch Head Shepherd can filter
    session.scans.push({
      memberId: member._id,
      memberName: member.fullName,
      group: member.group,
      branch: member.branch || req.user.branch || "MOR Head Quarter",
      membershipStatus: member.membershipStatus,
      scanTime: now,
      timing,
    });
    await session.save();

    // ─── Write into the Attendance register ──────────────────────────────
    const dateStr = session.date.toISOString().split("T")[0];
    // If session has no group (Head Shepherd QR) use the member's own group
    const effectiveGroup = session.group || member.group;

    const attQuery =
      session.type === "cbs"
        ? {
            type: "cbs",
            cbsLocation: session.cbsLocation,
            date: {
              $gte: new Date(dateStr + "T00:00:00.000Z"),
              $lt: new Date(dateStr + "T23:59:59.999Z"),
            },
          }
        : {
            type: session.type,
            group: effectiveGroup,
            date: {
              $gte: new Date(dateStr + "T00:00:00.000Z"),
              $lt: new Date(dateStr + "T23:59:59.999Z"),
            },
          };

    let attendance = await Attendance.findOne(attQuery);

    if (!attendance) {
      // ── Create a brand-new attendance record for this session date ──────
      // Seed it with all members of the effective group (all absent by default)
      const groupMembers = effectiveGroup
        ? await Member.find({ group: effectiveGroup }).select("_id fullName")
        : [];

      const initialRecords = groupMembers.map((m) => ({
        memberId: m._id,
        memberName: m.fullName,
        status:
          m._id.toString() === member._id.toString() ? "present" : "absent",
        checkInTime: m._id.toString() === member._id.toString() ? now : null,
        scanMethod:
          m._id.toString() === member._id.toString() ? "qr" : "manual",
      }));

      // If member not in group members list add them anyway
      if (
        !groupMembers.find((m) => m._id.toString() === member._id.toString())
      ) {
        initialRecords.push({
          memberId: member._id,
          memberName: member.fullName,
          status: "present",
          checkInTime: now,
          scanMethod: "qr",
        });
      }

      const presentCount = initialRecords.filter(
        (r) => r.status === "present",
      ).length;
      attendance = new Attendance({
        type: session.type,
        group: effectiveGroup,
        cbsLocation: session.cbsLocation || null,
        branch:
          session.branch ||
          member.branch ||
          req.user.branch ||
          "MOR Head Quarter",
        date: session.date,
        records: initialRecords,
        stats: {
          total: initialRecords.length,
          present: presentCount,
          absent: initialRecords.length - presentCount,
          percentage: Math.round((presentCount / initialRecords.length) * 100),
        },
        recordedByName: "QR Scan",
      });
      await attendance.save();
    } else {
      // ── Update existing attendance record ────────────────────────────────
      const existingRecord = attendance.records.find(
        (r) =>
          r.memberName === member.fullName ||
          r.memberId?.toString() === member._id.toString(),
      );
      if (existingRecord) {
        existingRecord.status = "present";
        existingRecord.checkInTime = now;
        existingRecord.scanMethod = "qr";
      } else {
        attendance.records.push({
          memberId: member._id,
          memberName: member.fullName,
          status: "present",
          checkInTime: now,
          scanMethod: "qr",
        });
      }
      const presentCount = attendance.records.filter(
        (r) => r.status === "present",
      ).length;
      attendance.stats = {
        total: attendance.records.length,
        present: presentCount,
        absent: attendance.records.length - presentCount,
        percentage: Math.round(
          (presentCount / attendance.records.length) * 100,
        ),
      };
      await attendance.save();
    }

    await logActivity(
      `scanned QR attendance (${session.type}) — ${timing === "on_time" ? "on time" : "late"}`,
      req.user,
    );

    res.json({
      message: `Attendance recorded! You are ${timing === "on_time" ? "On Time" : "Late"}`,
      timing,
      memberName: member.fullName,
      group: member.group || effectiveGroup,
      membershipStatus: member.membershipStatus,
      phoneNumber: member.phoneNumber,
      scanTime: now,
      sessionType: session.type,
      sessionDate: session.date,
      cbsLocation: session.cbsLocation || null,
    });
  } catch (error) {
    console.error("QR scan error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.get("/api/qr/sessions", authMiddleware, async (req, res) => {
  try {
    let query = {};
    if (req.user.role === "Group Leader") {
      // Show sessions this leader created for their group,
      // OR any session (fellowship, cbs, evangelism) generated by the Head Shepherd
      // where at least one member of their group has scanned.
      query = {
        $or: [{ group: req.user.group }, { "scans.group": req.user.group }],
      };
    } else if (req.user.role === "Branch Head Shepherd") {
      // Show sessions created by this branch, created by this user,
      // OR any session where a member from this branch has scanned
      if (req.user.branch) {
        query = {
          $or: [
            { branch: req.user.branch },
            { createdBy: req.user._id },
            { "scans.branch": req.user.branch },
          ],
        };
      } else {
        query.createdBy = req.user._id;
      }
    }
    const sessions = await QRSession.find(query)
      .sort({ createdAt: -1 })
      .limit(50);
    res.json(sessions);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.get("/api/qr/sessions/:id", authMiddleware, async (req, res) => {
  try {
    const session = await QRSession.findById(req.params.id);
    if (!session) return res.status(404).json({ error: "Session not found" });
    res.json(session);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== FOLLOW-UP ASSIGNMENTS ==========
app.get("/api/assignments", authMiddleware, async (req, res) => {
  try {
    let query = {};
    if (req.user.role === "Group Leader") query.group = req.user.group;
    else if (req.user.role === "Branch Head Shepherd")
      query.branch = req.user.branch;
    else if (req.user.role === "Member" && !req.query.assignedTo) {
      // Ordinary member viewing their own follow-up leader.
      // Skip when ?assignedTo is passed — that means a steward/leader with Member role
      // is fetching their own assigned members; the block below handles it.
      const memberRecord = await Member.findOne({
        phoneNumber: req.user.phoneNumber,
      });
      const ids = [req.user._id];
      if (
        memberRecord &&
        memberRecord._id.toString() !== req.user._id.toString()
      ) {
        ids.push(memberRecord._id);
      }
      query.member = { $in: ids };
    }
    // Explicit query overrides from request (for steward/leader fetching their assigned members)
    if (req.query.assignedTo) {
      // When a steward/leader queries their own assignments, resolve both User and Member IDs
      const reqId = req.query.assignedTo;
      const memberRecord = await Member.findById(reqId).catch(() => null);
      const userRecord = await User.findById(reqId).catch(() => null);
      const ids = [reqId];
      if (memberRecord) {
        // Also find the User record for this member
        const linkedUser = await User.findOne({
          phoneNumber: memberRecord.phoneNumber,
        }).catch(() => null);
        if (linkedUser) ids.push(linkedUser._id.toString());
      }
      if (userRecord) {
        // Also find the Member record for this user
        const linkedMember = await Member.findOne({
          phoneNumber: userRecord.phoneNumber,
        }).catch(() => null);
        if (linkedMember) ids.push(linkedMember._id.toString());
      }
      query.assignedTo = { $in: [...new Set(ids)] };
    }
    if (req.query.group) query.group = req.query.group;
    const assignments = await Assignment.find(query).sort({ createdAt: -1 });
    res.json(assignments);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post(
  "/api/assignments",
  authMiddleware,
  roleMiddleware(
    "Head Shepherd",
    "Branch Head Shepherd",
    "Group Leader",
    "System Admin",
  ),
  async (req, res) => {
    try {
      const { assignedToId, memberId, notes } = req.body;
      if (!assignedToId || !memberId)
        return res
          .status(400)
          .json({ error: "assignedToId and memberId required" });

      // Look up leader/steward: try Member first (dropdown sends Member._id),
      // then fall back to User for cases where IDs align
      let assignedToMember = await Member.findById(assignedToId).catch(
        () => null,
      );
      let assignedToUser = await User.findById(assignedToId).catch(() => null);

      // If found as Member but not User, look up User by phone for the _id reference
      if (assignedToMember && !assignedToUser) {
        assignedToUser = await User.findOne({
          phoneNumber: assignedToMember.phoneNumber,
        }).catch(() => null);
      }
      if (!assignedToMember && !assignedToUser)
        return res.status(404).json({ error: "Leader/Steward not found" });

      // Use whichever record we have for name/role
      const assignedToName =
        assignedToMember?.fullName || assignedToUser?.fullName || "Unknown";
      const assignedToRole =
        assignedToMember?.membershipStatus === "Leader" ? "Leader" : "Steward";
      const assignedToStatus = assignedToMember?.membershipStatus || "";
      // Use the User _id if available (for DB ref), else use the Member _id
      const assignedToRef = assignedToUser?._id || assignedToMember?._id;

      const member = await Member.findById(memberId);
      if (!member) return res.status(404).json({ error: "Member not found" });
      if (req.user.role === "Group Leader" && member.group !== req.user.group)
        return res
          .status(403)
          .json({ error: "You can only assign members in your own group" });
      const existing = await Assignment.findOne({
        assignedTo: assignedToRef,
        member: memberId,
      });
      if (existing)
        return res
          .status(409)
          .json({ error: "This member is already assigned to this person" });
      const assignment = new Assignment({
        assignedBy: req.user._id,
        assignedByName: req.user.fullName,
        assignedTo: assignedToRef,
        assignedToName,
        assignedToRole,
        assignedToStatus,
        member: member._id,
        memberName: member.fullName,
        memberPhone: member.phoneNumber,
        memberStatus: member.membershipStatus,
        group: member.group,
        branch: member.branch || "MOR Head Quarter",
        notes: notes || "",
      });
      await assignment.save();
      await Member.findByIdAndUpdate(memberId, {
        assignedTo: assignedToRef,
        assignedToName,
      });
      await logActivity(
        `assigned ${member.fullName} to ${assignedToName} for follow-up`,
        req.user,
      );
      res.status(201).json(assignment);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.delete(
  "/api/assignments/:id",
  authMiddleware,
  roleMiddleware(
    "Head Shepherd",
    "Branch Head Shepherd",
    "Group Leader",
    "System Admin",
  ),
  async (req, res) => {
    try {
      const assignment = await Assignment.findById(req.params.id);
      if (!assignment)
        return res.status(404).json({ error: "Assignment not found" });
      await Member.findByIdAndUpdate(assignment.member, {
        assignedTo: null,
        assignedToName: null,
      });
      await Assignment.findByIdAndDelete(req.params.id);
      res.json({ message: "Assignment removed" });
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== REPORTS ==========
app.get("/api/reports", authMiddleware, async (req, res) => {
  try {
    let query = {};
    if (req.user.role === "Group Leader") query.targetGroup = req.user.group;
    else if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      // Show reports targeted at this branch OR general system reports
      query = {
        $or: [{ targetBranch: req.user.branch }, { scope: "general" }],
      };
    }
    const reports = await Report.find(query).sort({ createdAt: -1 }).limit(100);
    res.json(reports);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/reports", authMiddleware, async (req, res) => {
  try {
    const { title, body, type, targetGroup, targetBranch } = req.body;
    const scope = targetGroup ? "group" : targetBranch ? "branch" : "general";
    const sentToUsers = await User.find(
      { role: { $in: ["Head Shepherd", "System Admin"] } },
      { _id: 1, fullName: 1, role: 1 },
    );
    const report = new Report({
      title,
      body,
      type: type || "manual",
      scope,
      targetGroup,
      targetBranch,
      sentBy: req.user._id,
      sentByName: req.user.fullName,
      sentTo: sentToUsers.map((u) => ({
        userId: u._id,
        name: u.fullName,
        role: u.role,
      })),
      period: {
        start: new Date(new Date().getFullYear(), 0, 1),
        end: new Date(),
      },
    });
    await report.save();
    await sendSystemNotification(
      `📋 New Report: ${title}`,
      `${req.user.fullName} submitted a report: ${title}`,
      "report",
    );
    res.status(201).json(report);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

app.put("/api/reports/:id/read", authMiddleware, async (req, res) => {
  try {
    await Report.findByIdAndUpdate(req.params.id, {
      $addToSet: { readBy: req.user._id },
    });
    res.json({ message: "Marked as read" });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== NOTIFICATION SCHEDULES ==========
app.get("/api/notification-schedules", authMiddleware, async (req, res) => {
  try {
    let query = {};
    if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      // Branch shepherd sees only their own branch schedules
      query = {
        $or: [{ targetBranch: req.user.branch }, { createdBy: req.user._id }],
      };
    }
    res.json(await NotifSchedule.find(query).sort({ createdAt: -1 }));
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});
app.post(
  "/api/notification-schedules",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Branch Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const schedData = { ...req.body, createdBy: req.user._id };
      // Tag with branch for Branch Head Shepherd
      if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
        schedData.targetBranch = req.user.branch;
        schedData.targetScope = "branch";
      }
      const s = new NotifSchedule(schedData);
      await s.save();
      res.status(201).json(s);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);
app.put(
  "/api/notification-schedules/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Branch Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const s = await NotifSchedule.findById(req.params.id);
      if (!s) return res.status(404).json({ error: "Schedule not found" });
      // Branch Head Shepherd can only update their own branch's schedules
      if (
        req.user.role === "Branch Head Shepherd" &&
        s.targetBranch !== req.user.branch &&
        s.createdBy?.toString() !== req.user._id.toString()
      )
        return res.status(403).json({ error: "Access denied" });
      const updated = await NotifSchedule.findByIdAndUpdate(
        req.params.id,
        req.body,
        { new: true },
      );
      res.json(updated);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);
app.delete(
  "/api/notification-schedules/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Branch Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const s = await NotifSchedule.findById(req.params.id);
      if (!s) return res.status(404).json({ error: "Schedule not found" });
      if (
        req.user.role === "Branch Head Shepherd" &&
        s.targetBranch !== req.user.branch &&
        s.createdBy?.toString() !== req.user._id.toString()
      )
        return res.status(403).json({ error: "Access denied" });
      await NotifSchedule.findByIdAndDelete(req.params.id);
      res.json({ message: "Schedule deleted" });
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== NOTIFICATIONS ==========
app.get("/api/notifications", authMiddleware, async (req, res) => {
  try {
    let query = {};
    if (req.user.role === "Member") {
      query = {
        $or: [
          { type: "general" },
          { type: "reminder" },
          { type: "group", targetGroup: req.user.group },
          { type: "personal", targetUser: req.user._id },
        ],
      };
    } else if (req.user.role === "Group Leader") {
      query = {
        $or: [
          { type: "general" },
          { type: "reminder" },
          { type: "group", targetGroup: req.user.group },
          { type: "report" },
        ],
      };
    } else if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      // Only see notifications relevant to their branch
      const branchGroups = await Group.find({ branch: req.user.branch }).select(
        "name",
      );
      const groupNames = branchGroups.map((g) => g.name);
      query = {
        $or: [
          { targetBranch: req.user.branch },
          { type: "group", targetGroup: { $in: groupNames } },
          { type: "reminder", targetBranch: req.user.branch },
          { type: "reminder", targetScope: "all" },
          { sentBy: req.user._id },
        ],
      };
    }
    const notifications = await Notification.find(query)
      .sort({ createdAt: -1 })
      .limit(50);
    res.json(
      notifications.map((n) => ({
        ...n.toObject(),
        isRead: n.readBy.includes(req.user._id),
      })),
    );
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});
app.post("/api/notifications", authMiddleware, async (req, res) => {
  try {
    if (req.user.role === "Member") {
      req.body.type = "group";
      req.body.targetGroup = req.user.group;
    }
    // Tag notification with sender's branch so recipients can filter
    if (req.user.branch) req.body.targetBranch = req.user.branch;
    const notification = new Notification({
      ...req.body,
      sentBy: req.user._id,
      sentByName: req.user.fullName,
      sentByRole: req.user.role,
    });
    await notification.save();
    res.status(201).json(notification);
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});
app.post("/api/notifications/:id/read", authMiddleware, async (req, res) => {
  try {
    const n = await Notification.findById(req.params.id);
    if (!n) return res.status(404).json({ error: "Notification not found" });
    if (!n.readBy.includes(req.user._id)) {
      n.readBy.push(req.user._id);
      await n.save();
    }
    res.json({ message: "Marked as read" });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});
app.delete("/api/notifications/:id", authMiddleware, async (req, res) => {
  try {
    const n = await Notification.findById(req.params.id);
    if (!n) return res.status(404).json({ error: "Notification not found" });
    if (
      n.sentBy?.toString() !== req.user._id.toString() &&
      !["Head Shepherd", "System Admin"].includes(req.user.role)
    )
      return res
        .status(403)
        .json({ error: "You can only delete your own notifications" });
    await Notification.findByIdAndDelete(req.params.id);
    res.json({ message: "Notification deleted" });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== MEDIA ROUTES ==========
app.get("/api/media", authMiddleware, async (req, res) => {
  try {
    let query = {};
    if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      query.branch = req.user.branch;
    }
    res.json(await Media.find(query).sort({ createdAt: -1 }));
  } catch (e) {
    res.status(500).json({ error: "Server error" });
  }
});
app.post(
  "/api/media",
  authMiddleware,
  mediaUpload.array("files", 20),
  async (req, res) => {
    try {
      const { title, type, description } = req.body;
      if (!type) return res.status(400).json({ error: "Type required" });
      const files =
        req.files && req.files.length ? req.files : req.file ? [req.file] : [];
      if (!files.length && !title)
        return res.status(400).json({ error: "Title and file required" });

      const branch = req.user.branch || null;
      const savedMedia = [];

      // Support multiple files
      const fileList = files.length ? files : [null];
      for (let i = 0; i < fileList.length; i++) {
        const file = fileList[i];
        const mediaTitle =
          files.length > 1 ? `${title || type} (${i + 1})` : title || type;
        let fileInfo = {};
        if (file) {
          const cloudinaryUrl = await new Promise((resolve, reject) => {
            const stream = cloudinary.uploader.upload_stream(
              { folder: "mor-system/media", resource_type: "auto" },
              (error, result) => {
                if (error) return reject(error);
                resolve(result.secure_url);
              },
            );
            stream.end(file.buffer);
          });
          fileInfo = {
            fileName: file.originalname,
            filePath: cloudinaryUrl,
            fileSize: file.size,
            mimeType: file.mimetype,
          };
        }
        const media = new Media({
          title: mediaTitle,
          type,
          description,
          branch,
          ...fileInfo,
          uploadedBy: req.user._id,
          uploadedByName: req.user.fullName,
        });
        await media.save();
        savedMedia.push(media);
      }
      res
        .status(201)
        .json(savedMedia.length === 1 ? savedMedia[0] : savedMedia);
    } catch (error) {
      res.status(500).json({ error: "Server error" });
    }
  },
);
app.delete("/api/media/:id", authMiddleware, async (req, res) => {
  try {
    const media = await Media.findById(req.params.id);
    if (!media) return res.status(404).json({ error: "Media not found" });
    await Media.findByIdAndDelete(req.params.id);
    res.json({ message: "Media deleted" });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== ACTIVITY LOGS ==========
app.get("/api/activity-logs", authMiddleware, async (req, res) => {
  try {
    const { limit = 100 } = req.query;
    let query = {};
    // Branch Head Shepherd sees their branch logs + any logs where branch is unset
    if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      query = {
        $or: [{ branch: req.user.branch }, { branch: null }, { branch: "" }],
      };
    } else if (req.user.role === "Group Leader") {
      // Group leaders see all logs — client filters by group member names
      query = {};
    }
    res.json(
      await ActivityLog.find(query)
        .sort({ createdAt: -1 })
        .limit(parseInt(limit)),
    );
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});
app.delete("/api/activity-logs", authMiddleware, async (req, res) => {
  try {
    await ActivityLog.deleteMany({});
    res.json({ message: "Activity logs cleared" });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== DEBUG ENDPOINT ==========
app.get("/api/debug/user", authMiddleware, async (req, res) => {
  try {
    const user = await User.findById(req.user._id).select("-password");
    res.json({
      id: user._id,
      fullName: user.fullName,
      phoneNumber: user.phoneNumber,
      role: user.role,
      branch: user.branch,
      isBranchShepherd: user.isBranchShepherd,
      isGroupLeader: user.isGroupLeader,
      isCBSLeader: user.isCBSLeader,
      isSteward: user.isSteward,
    });
  } catch (error) {
    res.status(500).json({ error: "Server error" });
  }
});

// ========== AUTOMATED CRON JOBS ==========

// ── Quarterly Membership Status Auto-Update Engine ────────────────────────
// Rules (based on 12 counted fellowship Saturdays per quarter):
//   UP:   First Timer → Inconsistent     : >= 60%  (auto)
//         Inconsistent → Semi-Consistent : >= 60%  (auto)
//         Semi-Consistent → Consistent   : >= 70%  (auto)
//         Consistent → Intense Leader    : >= 90%  (PENDING — GL approval)
//         Intense Leader → Leader        : = 100%  (PENDING — GL approval)
//   DOWN: Semi-Consistent → Inconsistent : <= 40%  (auto)
//         Consistent → Inconsistent      : <= 40%  (auto)  [skips Semi]
//         Intense Leader → Consistent    : <= 40%  (PENDING — GL approval)
//         Leader → Intense Leader        : <= 40%  (PENDING — GL approval)
//   NOTE: First Timer is excluded from DOWN rules.
//         "Discipleship" status is excluded from both — handled manually.
const STATUS_ORDER = [
  "First Timer", // 0
  "Inconsistent", // 1
  "Semi-Consistent", // 2
  "Consistent", // 3
  "Intense Leader", // 4
  "Leader", // 5
];
const SESSIONS_PER_QUARTER = 13; // Full 13 Saturdays per quarter (no deductions)

function getQuarterLabel(date) {
  const m = date.getMonth();
  const q = Math.floor(m / 3) + 1;
  return `Q${q} ${date.getFullYear()}`;
}

function getQuarterDateRange(date) {
  const year = date.getFullYear();
  const q = Math.floor(date.getMonth() / 3);
  const starts = [
    new Date(year, 0, 1),
    new Date(year, 3, 1),
    new Date(year, 6, 1),
    new Date(year, 9, 1),
  ];
  const ends = [
    new Date(year, 2, 31, 23, 59, 59),
    new Date(year, 5, 30, 23, 59, 59),
    new Date(year, 8, 30, 23, 59, 59),
    new Date(year, 11, 31, 23, 59, 59),
  ];
  return { start: starts[q], end: ends[q] };
}

// Check if member needs UP promotion
function evalStatusUp(status, pct) {
  // Returns { toStatus, pending } or null
  if (status === "First Timer" && pct >= 60)
    return { toStatus: "Inconsistent", pending: false };
  if (status === "Inconsistent" && pct >= 60)
    return { toStatus: "Semi-Consistent", pending: false };
  if (status === "Semi-Consistent" && pct >= 70)
    return { toStatus: "Consistent", pending: false };
  if (status === "Consistent" && pct >= 90)
    return { toStatus: "Intense Leader", pending: true };
  if (status === "Intense Leader" && pct >= 100)
    return { toStatus: "Leader", pending: true };
  return null;
}

// Check if member needs DOWN demotion
function evalStatusDown(status, pct) {
  // First Timer excluded. Discipleship excluded.
  if (status === "First Timer" || status === "Discipleship") return null;
  if (status === "Semi-Consistent" && pct <= 40)
    return { toStatus: "Inconsistent", pending: false };
  if (status === "Consistent" && pct <= 40)
    return { toStatus: "Inconsistent", pending: false };
  if (status === "Intense Leader" && pct <= 40)
    return { toStatus: "Consistent", pending: true };
  if (status === "Leader" && pct <= 40)
    return { toStatus: "Intense Leader", pending: true };
  return null;
}

async function runQuarterlyStatusUpdate(quarterDate) {
  const quarterLabel = getQuarterLabel(quarterDate);
  const { start, end } = getQuarterDateRange(quarterDate);
  console.log(
    `⚙️  Running quarterly status update for ${quarterLabel} (${start.toDateString()} – ${end.toDateString()})`,
  );

  try {
    const groups = await Group.find();
    let totalProcessed = 0,
      totalUpdated = 0,
      totalPending = 0;

    for (const group of groups) {
      const members = await Member.find({
        group: group.name,
        membershipStatus: { $in: STATUS_ORDER },
      });
      if (!members.length) continue;

      // Fetch all fellowship Saturday sessions in this quarter for this group
      const sessions = await Attendance.find({
        type: "fellowship",
        group: group.name,
        date: { $gte: start, $lte: end },
      });

      // Cap at SESSIONS_PER_QUARTER (12) — use actual count but max 12
      const totalSessions = Math.min(sessions.length, SESSIONS_PER_QUARTER);
      if (totalSessions === 0) continue;

      // Find group leader user for pending notifications
      const groupLeaderUser = await User.findOne({
        group: group.name,
        role: "Group Leader",
      });
      // Find head shepherd(s)
      const headShepherds = await User.find({
        role: { $in: ["Head Shepherd", "Branch Head Shepherd"] },
      });

      for (const member of members) {
        totalProcessed++;
        const attended = sessions.filter((s) =>
          s.records.some(
            (r) => r.memberName === member.fullName && r.status === "present",
          ),
        ).length;

        const pct = Math.min(
          100,
          parseFloat(((attended / totalSessions) * 100).toFixed(1)),
        );

        // Check UP first, then DOWN
        const upResult = evalStatusUp(member.membershipStatus, pct);
        const downResult = upResult
          ? null
          : evalStatusDown(member.membershipStatus, pct);
        const result = upResult || downResult;
        if (!result) continue;

        const direction = upResult ? "up" : "down";

        // Avoid duplicate entries for same member + quarter
        const existing = await StatusUpdate.findOne({
          memberId: member._id,
          quarter: quarterLabel,
          toStatus: result.toStatus,
        });
        if (existing) continue;

        // Create the status update record
        const updateStatus = result.pending ? "pending" : "auto";
        const record = await StatusUpdate.create({
          memberId: member._id,
          memberName: member.fullName,
          memberPhone: member.phoneNumber,
          group: group.name,
          branch: member.branch || group.branch || "MOR Head Quarter",
          fromStatus: member.membershipStatus,
          toStatus: result.toStatus,
          direction,
          attendancePct: pct,
          attended,
          totalSessions,
          quarter: quarterLabel,
          status: updateStatus,
          appliedAt: updateStatus === "auto" ? new Date() : undefined,
        });

        // Apply immediately for auto updates
        if (updateStatus === "auto") {
          await Member.findByIdAndUpdate(member._id, {
            membershipStatus: result.toStatus,
          });
          await User.findOneAndUpdate(
            { phoneNumber: member.phoneNumber },
            { membershipStatus: result.toStatus },
          );
          totalUpdated++;
          // ── Log to StatusChangeLog ──
          await writeStatusLog({
            memberId: member._id,
            memberName: member.fullName,
            memberPhone: member.phoneNumber,
            group: group.name,
            branch: member.branch || group.branch || "MOR Head Quarter",
            fromStatus: member.membershipStatus,
            toStatus: result.toStatus,
            direction,
            attendancePct: pct,
            attended,
            totalSessions,
            quarter: quarterLabel,
            changeType: "auto",
            statusUpdateRef: record._id,
          });
        } else {
          totalPending++;
        }

        // ── Notification messages ────────────────────────────────────────
        const dirEmoji = direction === "up" ? "📈" : "📉";
        const dirWord = direction === "up" ? "promoted" : "demoted";
        const pendNote = result.pending ? " (Pending GL approval)" : "";
        const pctStr = `${attended}/${totalSessions} (${pct}%)`;

        const memberTitle = `${dirEmoji} Your Status Update${pendNote}`;
        const memberMsg = result.pending
          ? `Your fellowship attendance for ${quarterLabel} was ${pctStr}. A status change from "${member.membershipStatus}" to "${result.toStatus}" has been submitted and is pending your Group Leader's approval.`
          : `Based on your fellowship attendance for ${quarterLabel} (${pctStr}), your membership status has been updated from "${member.membershipStatus}" to "${result.toStatus}".`;

        const leaderTitle = `${dirEmoji} Status Update — ${member.fullName}${pendNote}`;
        const leaderMsg = result.pending
          ? `${member.fullName} (${group.name} Group) has a pending status change from "${member.membershipStatus}" to "${result.toStatus}" for ${quarterLabel} (attendance: ${pctStr}). Please review and approve or reject in the Status Updates section.`
          : `${member.fullName} in ${group.name} Group has been ${dirWord} from "${member.membershipStatus}" to "${result.toStatus}" for ${quarterLabel} (attendance: ${pctStr}).`;

        // Notify the member (by phone/user account)
        const memberUser = await User.findOne({
          phoneNumber: member.phoneNumber,
        });
        if (memberUser) {
          await sendSystemNotification(
            memberTitle,
            memberMsg,
            "personal",
            null,
            memberUser._id,
          );
        }

        // Notify the group leader
        if (groupLeaderUser) {
          await sendSystemNotification(
            leaderTitle,
            leaderMsg,
            "group",
            group.name,
            groupLeaderUser._id,
          );
        }

        // Notify head shepherds
        for (const hs of headShepherds) {
          await sendSystemNotification(
            leaderTitle,
            leaderMsg,
            "report",
            null,
            hs._id,
          );
        }

        await logActivity(
          `STATUS_${direction.toUpperCase()}${result.pending ? "_PENDING" : "_AUTO"}`,
          null,
          `${member.fullName} (${group.name}): ${member.membershipStatus} → ${result.toStatus} | ${pctStr} | ${quarterLabel}${result.pending ? " [PENDING]" : ""}`,
        );
      }
    }

    console.log(
      `✅ Quarterly status update complete: ${totalProcessed} checked, ${totalUpdated} auto-updated, ${totalPending} pending approval`,
    );
    return { totalProcessed, totalUpdated, totalPending };
  } catch (e) {
    console.error("❌ Quarterly status update error:", e.message);
    throw e;
  }
}

// Run on the 1st day of Jan, Apr, Jul, Oct at 08:00 server time (start of next quarter)
cron.schedule("0 8 1 1,4,7,10 *", async () => {
  const prevQtrDate = new Date();
  prevQtrDate.setMonth(prevQtrDate.getMonth() - 1); // roll back to the quarter just ended
  const prevQtrLabel = getQuarterLabel(prevQtrDate);

  // ── Expire any pending StatusUpdate records from the quarter that just closed ──
  // A pending record that reaches quarter-end without GL approval means the member
  // stays at their original (fromStatus) — no status change is applied.
  // We mark the record as "rejected" with an automatic note so it no longer
  // appears as actionable and the UI can display the correct outcome.
  try {
    const expiredPending = await StatusUpdate.find({
      quarter: prevQtrLabel,
      status: "pending",
    });
    for (const rec of expiredPending) {
      rec.status = "rejected";
      rec.rejectionNote =
        "Quarter ended without approval — member kept original status (" +
        rec.fromStatus +
        ").";
      rec.reviewedAt = new Date();
      await rec.save();
      // Log the expiry so the Status Change Log reflects it
      await writeStatusLog({
        memberId: rec.memberId,
        memberName: rec.memberName,
        memberPhone: rec.memberPhone,
        group: rec.group,
        branch: rec.branch,
        fromStatus: rec.fromStatus,
        toStatus: rec.fromStatus, // kept at original
        direction: "none",
        attendancePct: rec.attendancePct,
        attended: rec.attended,
        totalSessions: rec.totalSessions,
        quarter: prevQtrLabel,
        changeType: "auto",
        statusUpdateRef: rec._id,
      });
      console.log(
        `⏰ Expired pending: ${rec.memberName} stayed at ${rec.fromStatus} (${prevQtrLabel})`,
      );
    }
    if (expiredPending.length)
      console.log(
        `⏰ Expired ${expiredPending.length} pending record(s) for ${prevQtrLabel}`,
      );
  } catch (e) {
    console.error("❌ Error expiring pending records:", e.message);
  }

  await runQuarterlyStatusUpdate(prevQtrDate);
});

// ── Status Update API endpoints ──────────────────────────────────────────────

// GET /api/status-updates — list for the requesting GL's group (or all for HS)
app.get("/api/status-updates", authMiddleware, async (req, res) => {
  try {
    const { quarter, status, group } = req.query;
    const query = {};
    if (req.user.role === "Group Leader") {
      query.group = req.user.group;
    } else if (req.user.role === "Branch Head Shepherd") {
      query.branch = req.user.branch;
    }
    if (quarter) query.quarter = quarter;
    if (status) query.status = status;
    if (
      group &&
      ["Head Shepherd", "Branch Head Shepherd", "System Admin"].includes(
        req.user.role,
      )
    ) {
      query.group = group;
    }
    const updates = await StatusUpdate.find(query)
      .sort({ createdAt: -1 })
      .limit(300);
    res.json(updates);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/status-updates — create a pending record from the GL frontend.
// The quarterly engine normally does this via cron, but the GL register
// also creates pending records on-the-fly so the Review button appears
// without waiting for the cron. Duplicates are silently ignored.
app.post(
  "/api/status-updates",
  authMiddleware,
  roleMiddleware(
    "Group Leader",
    "Head Shepherd",
    "Branch Head Shepherd",
    "System Admin",
  ),
  async (req, res) => {
    try {
      const {
        memberId,
        memberName,
        memberPhone,
        group,
        branch,
        fromStatus,
        toStatus,
        direction,
        attendancePct,
        attended,
        totalSessions,
        quarter,
        status,
      } = req.body;

      // Only allow pending records through this endpoint — auto records are
      // written by the engine or the status-change-log endpoint.
      if (status !== "pending") {
        return res
          .status(400)
          .json({ error: "Only pending records can be created here." });
      }

      // Prevent duplicates: same member + quarter + toStatus
      const existing = await StatusUpdate.findOne({
        memberId,
        quarter,
        toStatus,
      });
      if (existing) return res.json(existing); // return the existing record silently

      const record = await StatusUpdate.create({
        memberId,
        memberName,
        memberPhone,
        group: group || req.user.group,
        branch: branch || req.user.branch || "MOR Head Quarter",
        fromStatus,
        toStatus,
        direction,
        attendancePct,
        attended,
        totalSessions: totalSessions || 13,
        quarter,
        status: "pending",
      });
      res.status(201).json(record);
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  },
);

// POST /api/status-updates/run — manual trigger (HS/Admin only)
app.post(
  "/api/status-updates/run",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { quarter } = req.body; // optional: "Q1 2025" → parse year+qtr
      let targetDate = new Date();
      if (quarter) {
        const [q, yr] = quarter.split(" ");
        const qIdx = { Q1: 0, Q2: 1, Q3: 2, Q4: 3 }[q] || 0;
        targetDate = new Date(parseInt(yr), qIdx * 3 + 1, 1);
      } else {
        targetDate.setMonth(targetDate.getMonth() - 1);
      }
      const result = await runQuarterlyStatusUpdate(targetDate);
      res.json({ success: true, ...result });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  },
);

// PATCH /api/status-updates/:id/review — GL approves or rejects a pending update
app.patch(
  "/api/status-updates/:id/review",
  authMiddleware,
  roleMiddleware("Group Leader", "Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { action, note } = req.body; // action: "approve" | "reject"
      const record = await StatusUpdate.findById(req.params.id);
      if (!record) return res.status(404).json({ error: "Record not found" });
      if (record.status !== "pending")
        return res.status(400).json({ error: "Not pending" });
      if (req.user.role === "Group Leader" && record.group !== req.user.group)
        return res.status(403).json({ error: "Not your group" });

      record.status = action === "approve" ? "approved" : "rejected";
      record.reviewedBy = req.user._id;
      record.reviewedByName = req.user.fullName;
      record.reviewedAt = new Date();
      record.rejectionNote = action === "reject" ? note || "" : undefined;
      if (action === "approve") record.appliedAt = new Date();
      await record.save();

      if (action === "approve") {
        await Member.findByIdAndUpdate(record.memberId, {
          membershipStatus: record.toStatus,
        });
        await User.findOneAndUpdate(
          { phoneNumber: record.memberPhone },
          { membershipStatus: record.toStatus },
        );
        // ── Log approved change to StatusChangeLog ──
        await writeStatusLog({
          memberId: record.memberId,
          memberName: record.memberName,
          memberPhone: record.memberPhone,
          group: record.group,
          branch: record.branch,
          fromStatus: record.fromStatus,
          toStatus: record.toStatus,
          direction: record.direction,
          attendancePct: record.attendancePct,
          attended: record.attended,
          totalSessions: record.totalSessions,
          quarter: record.quarter,
          changeType: "approved",
          changedBy: req.user._id,
          changedByName: req.user.fullName,
          statusUpdateRef: record._id,
        });
        // Notify member of approval
        const memberUser = await User.findOne({
          phoneNumber: record.memberPhone,
        });
        if (memberUser) {
          const dirEmoji = record.direction === "up" ? "📈" : "📉";
          await sendSystemNotification(
            `${dirEmoji} Status Update Approved`,
            `Your membership status has been updated from "${record.fromStatus}" to "${record.toStatus}" for ${record.quarter}. Approved by ${req.user.fullName}.`,
            "personal",
            null,
            memberUser._id,
          );
        }
        // Notify head shepherds
        const headShepherds = await User.find({
          role: { $in: ["Head Shepherd", "Branch Head Shepherd"] },
        });
        for (const hs of headShepherds) {
          await sendSystemNotification(
            `✅ Status Approved — ${record.memberName}`,
            `${req.user.fullName} approved the status change for ${record.memberName} (${record.group}): "${record.fromStatus}" → "${record.toStatus}" (${record.quarter}).`,
            "report",
            null,
            hs._id,
          );
        }
      } else {
        // Notify member of rejection
        const memberUser = await User.findOne({
          phoneNumber: record.memberPhone,
        });
        if (memberUser) {
          await sendSystemNotification(
            `ℹ️ Status Review Update`,
            `Your pending status change from "${record.fromStatus}" to "${record.toStatus}" for ${record.quarter} was reviewed by ${req.user.fullName}.${note ? " Note: " + note : ""}`,
            "personal",
            null,
            memberUser._id,
          );
        }
      }

      await logActivity(
        `STATUS_${action.toUpperCase()}`,
        req.user,
        `${record.memberName} (${record.group}): ${record.fromStatus} → ${record.toStatus} | ${record.quarter}`,
      );

      res.json({ success: true, record });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  },
);

// GET /api/status-updates/quarters — list distinct quarters available
app.get("/api/status-updates/quarters", authMiddleware, async (req, res) => {
  try {
    const quarters = await StatusUpdate.distinct("quarter");
    res.json(quarters.sort().reverse());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/status-change-log — fetch the audit log ─────────────────────────
// GL sees their own group; HS/Admin see all or filtered by group/quarter
app.get("/api/status-change-log", authMiddleware, async (req, res) => {
  try {
    const { quarter, group, changeType, limit = 500 } = req.query;
    const query = {};

    // Scope by role
    if (req.user.role === "Group Leader") {
      query.group = req.user.group;
    } else if (req.user.role === "Branch Head Shepherd") {
      query.branch = req.user.branch;
    }

    // Optional filters from query string
    if (quarter) query.quarter = quarter;
    if (changeType) query.changeType = changeType;
    if (
      group &&
      ["Head Shepherd", "Branch Head Shepherd", "System Admin"].includes(
        req.user.role,
      )
    ) {
      query.group = group;
    }

    const logs = await StatusChangeLog.find(query)
      .sort({ loggedAt: -1 })
      .limit(parseInt(limit));

    res.json(logs);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/status-change-log/quarters — distinct quarters in the log ─────────
app.get("/api/status-change-log/quarters", authMiddleware, async (req, res) => {
  try {
    const query = {};
    if (req.user.role === "Group Leader") query.group = req.user.group;
    else if (req.user.role === "Branch Head Shepherd")
      query.branch = req.user.branch;
    const quarters = await StatusChangeLog.distinct("quarter", query);
    res.json(quarters.sort().reverse());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/status-change-log — frontend-triggered save (e.g. register auto-detect) ──
// The register shows auto-detected changes visually; this endpoint saves them to the DB
// so they appear in the log even before the quarterly engine runs.
app.post(
  "/api/status-change-log",
  authMiddleware,
  roleMiddleware("Group Leader", "Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const {
        memberId,
        memberName,
        memberPhone,
        group,
        branch,
        fromStatus,
        toStatus,
        direction,
        attendancePct,
        attended,
        totalSessions,
        quarter,
        changeType,
        statusUpdateRef,
      } = req.body;

      // Prevent duplicate entries for same member + quarter + toStatus + changeType
      const existing = await StatusChangeLog.findOne({
        memberId,
        quarter,
        toStatus,
        changeType,
      });
      if (existing) return res.json({ duplicate: true, log: existing });

      // If changeType is "auto" — also apply to Member + User records immediately
      if (changeType === "auto") {
        const member = await Member.findById(memberId);
        if (!member) return res.status(404).json({ error: "Member not found" });
        if (req.user.role === "Group Leader" && member.group !== req.user.group)
          return res.status(403).json({ error: "Not your group" });

        await Member.findByIdAndUpdate(memberId, {
          membershipStatus: toStatus,
        });
        await User.findOneAndUpdate(
          { phoneNumber: memberPhone },
          { membershipStatus: toStatus },
        );
      }

      const log = await writeStatusLog({
        memberId,
        memberName,
        memberPhone,
        group: group || req.user.group,
        branch: branch || "MOR Head Quarter",
        fromStatus,
        toStatus,
        direction,
        attendancePct: attendancePct ?? null,
        attended: attended ?? null,
        totalSessions: totalSessions ?? null,
        quarter: quarter || currentQuarterLabel(),
        changeType: changeType || "manual",
        changedBy: req.user._id,
        changedByName: req.user.fullName,
        statusUpdateRef: statusUpdateRef || null,
      });

      await logActivity(
        `STATUS_LOG_${(changeType || "manual").toUpperCase()}`,
        req.user,
        `${memberName}: ${fromStatus} → ${toStatus} | ${quarter}`,
      );

      res.status(201).json({ success: true });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  },
);

cron.schedule("0 7 * * 1", async () => {
  try {
    console.log("⏰ Running weekly inconsistency check...");
    const fourWeeksAgo = new Date();
    fourWeeksAgo.setDate(fourWeeksAgo.getDate() - 28);
    const groups = await Group.find();
    for (const group of groups) {
      const members = await Member.find({ group: group.name });
      const [fellowshipAtt, cbsAtt, evangelismAtt] = await Promise.all([
        Attendance.find({
          type: "fellowship",
          group: group.name,
          date: { $gte: fourWeeksAgo },
        }),
        Attendance.find({ type: "cbs", date: { $gte: fourWeeksAgo } }),
        Attendance.find({
          type: "evangelism",
          group: group.name,
          date: { $gte: fourWeeksAgo },
        }),
      ]);
      const inconsistent = [];
      for (const member of members) {
        const fCount = fellowshipAtt.filter((a) =>
          a.records.some(
            (r) => r.memberName === member.fullName && r.status === "present",
          ),
        ).length;
        const cCount = cbsAtt.filter((a) =>
          a.records.some(
            (r) => r.memberName === member.fullName && r.status === "present",
          ),
        ).length;
        const eCount = evangelismAtt.filter((a) =>
          a.records.some(
            (r) => r.memberName === member.fullName && r.status === "present",
          ),
        ).length;
        const issues = [];
        if (fellowshipAtt.length > 0 && fCount / fellowshipAtt.length < 0.5)
          issues.push(`Fellowship: ${fCount}/${fellowshipAtt.length}`);
        if (cbsAtt.length > 0 && cCount / cbsAtt.length < 0.5)
          issues.push(`CBS: ${cCount}/${cbsAtt.length}`);
        if (evangelismAtt.length > 0 && eCount / evangelismAtt.length < 0.5)
          issues.push(`Evangelism: ${eCount}/${evangelismAtt.length}`);
        if (issues.length > 0)
          inconsistent.push({
            name: member.fullName,
            phone: member.phoneNumber,
            issues,
          });
      }
      if (inconsistent.length > 0) {
        const bodyText = inconsistent
          .map((m) => `• ${m.name} (${m.phone}): ${m.issues.join(", ")}`)
          .join("\n");
        await Report.create({
          title: `Weekly Inconsistency Report — ${group.name} Group`,
          type: "inconsistency",
          scope: "group",
          targetGroup: group.name,
          body: `Members with below 50% attendance in the last 4 weeks:\n\n${bodyText}`,
          data: { inconsistent },
          period: { start: fourWeeksAgo, end: new Date() },
          sentByName: "MOR System",
        });
        await sendSystemNotification(
          `⚠️ ${group.name} Inconsistency Report`,
          `${inconsistent.length} member(s) in ${group.name} group have inconsistent attendance.`,
          "report",
          group.name,
        );
      }
    }
    console.log("✅ Weekly inconsistency check complete");
  } catch (e) {
    console.error("Cron error:", e.message);
  }
});

cron.schedule("0 * * * *", async () => {
  try {
    const now = new Date();
    const schedules = await NotifSchedule.find({ isActive: true });
    for (const sched of schedules) {
      if (sched.schedule.hourUTC !== now.getUTCHours()) continue;
      if (
        sched.schedule.dayOfWeek !== undefined &&
        sched.schedule.dayOfWeek !== now.getDay()
      )
        continue;
      if (
        sched.schedule.month !== undefined &&
        sched.schedule.month !== now.getMonth() + 1
      )
        continue;
      if (
        sched.lastSent &&
        new Date(sched.lastSent).toDateString() === now.toDateString()
      )
        continue;
      await sendSystemNotification(
        sched.title,
        sched.message,
        "reminder",
        sched.targetGroup || null,
      );
      await NotifSchedule.findByIdAndUpdate(sched._id, { lastSent: now });
      console.log(`✅ Sent scheduled reminder: ${sched.title}`);
    }
  } catch (e) {
    console.error("Cron reminder error:", e.message);
  }
});

// ========== DATABASE INITIALIZATION ==========
async function initializeDatabase() {
  try {
    console.log("📦 Setting up database structure...");
    const branches = [
      "MOR Head Quarter",
      "MOR Eastern Branch",
      "MOR BO Branch",
    ];
    for (const branchName of branches) {
      if (!(await Branch.findOne({ name: branchName }))) {
        await Branch.create({
          name: branchName,
          description: branchName,
          isActive: true,
        });
        console.log(`   ✓ Created branch: ${branchName}`);
      }
    }
    const groups = ["Success", "Empowerment", "Zoe", "Favour", "Dominion"];
    for (const groupName of groups) {
      if (!(await Group.findOne({ name: groupName }))) {
        await Group.create({
          name: groupName,
          branch: "MOR Head Quarter",
          isActive: true,
          memberCount: 0,
          stewardCount: 0,
          description: `${groupName} Ministry Group`,
        });
        console.log(`   ✓ Created group: ${groupName}`);
      }
    }
    // CBS locations are no longer seeded automatically.
    // Each branch shepherd must register CBS locations manually via the dashboard.
    const defaultSchedules = [
      {
        title: "🙏 Fellowship Reminder",
        message:
          "Fellowship is today at 1 PM! Join us for worship, the Word, and fellowship together. Be there and be a blessing!",
        activityType: "fellowship",
        schedule: { dayOfWeek: 6, weekPattern: "every", hourUTC: 9 },
        isActive: true,
      },
      {
        title: "📖 CBS Reminder",
        message:
          "CBS Bible Study is tonight! Come and grow in the Word of God. Let nothing keep you away from studying God's Word.",
        activityType: "cbs",
        schedule: { dayOfWeek: 2, weekPattern: "every", hourUTC: 14 },
        isActive: true,
      },
      {
        title: "🕊 Evangelism Reminder",
        message:
          "Evangelism is today! Let us go out and share the Good News. Souls are waiting! Be part of this great commission.",
        activityType: "evangelism",
        schedule: { dayOfWeek: 5, weekPattern: "first", hourUTC: 14 },
        isActive: true,
      },
    ];
    for (const sched of defaultSchedules) {
      if (!(await NotifSchedule.findOne({ title: sched.title }))) {
        await NotifSchedule.create(sched);
        console.log(`   ✓ Created schedule: ${sched.title}`);
      }
    }
    console.log("✅ Database initialization complete!");
  } catch (error) {
    console.error("❌ Database initialization error:", error);
  }
}

// ═══════════════════════════════════════════════════════════════
// FOLLOW-UP CHAT ENDPOINTS
// ═══════════════════════════════════════════════════════════════

// GET  /api/followup-chat/:assignmentId  — fetch all messages for a thread
app.get(
  "/api/followup-chat/:assignmentId",
  authMiddleware,
  async (req, res) => {
    try {
      const assignment = await Assignment.findById(req.params.assignmentId);
      if (!assignment)
        return res.status(404).json({ error: "Assignment not found" });

      // Verify the requester has access to this chat thread
      // Allow: the assigned member, the assignedTo leader/steward, group leaders, branch shepherds, head shepherds
      const memberRecord = await Member.findOne({
        phoneNumber: req.user.phoneNumber,
      });
      const memberId = memberRecord?._id?.toString();
      const userId = req.user._id?.toString();
      const hasAccess =
        req.user.role === "Head Shepherd" ||
        req.user.role === "Branch Head Shepherd" ||
        req.user.role === "Group Leader" ||
        assignment.memberName === req.user.fullName ||
        assignment.assignedToName === req.user.fullName ||
        (memberId &&
          [
            assignment.member?.toString(),
            assignment.assignedTo?.toString(),
          ].includes(memberId)) ||
        (userId &&
          [
            assignment.member?.toString(),
            assignment.assignedTo?.toString(),
          ].includes(userId));

      if (!hasAccess) return res.status(403).json({ error: "Access denied" });

      const messages = await FollowUpChat.find({
        assignmentId: req.params.assignmentId,
      }).sort({ createdAt: 1 });
      res.json(messages);
    } catch (e) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// POST /api/followup-chat/:assignmentId  — send a message
app.post(
  "/api/followup-chat/:assignmentId",
  authMiddleware,
  async (req, res) => {
    try {
      const { message } = req.body;
      if (!message?.trim())
        return res.status(400).json({ error: "Message required" });
      const assignment = await Assignment.findById(req.params.assignmentId);
      if (!assignment)
        return res.status(404).json({ error: "Assignment not found" });

      // Resolve sender's Member record for consistent ID tracking
      const senderMember = await Member.findOne({
        phoneNumber: req.user.phoneNumber,
      });
      const senderId = senderMember?._id || req.user._id;
      const senderName = req.user.fullName;

      // Determine direction by matching sender name to assignment roles
      // This handles all cases: member, steward, leader, group-leader
      const isSenderTheMember = assignment.memberName === senderName;
      const senderRole = isSenderTheMember
        ? senderMember?.membershipStatus || "Member"
        : senderMember?.membershipStatus || req.user.role;

      let toMemberId, toName;
      if (isSenderTheMember) {
        // The person being followed up is sending → reply to their leader/steward
        toMemberId = assignment.assignedTo;
        toName = assignment.assignedToName;
      } else {
        // The leader/steward is sending → directed at the assigned member
        toMemberId = assignment.member;
        toName = assignment.memberName;
      }

      const msg = new FollowUpChat({
        assignmentId: req.params.assignmentId,
        fromMemberId: senderId,
        fromName: senderName,
        fromRole: senderRole,
        toMemberId,
        toName,
        message: message.trim(),
        readBy: [senderId], // sender has already read it
      });
      await msg.save();
      res.status(201).json(msg);
    } catch (e) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// PUT /api/followup-chat/:assignmentId/read  — mark all messages in thread as read
app.put(
  "/api/followup-chat/:assignmentId/read",
  authMiddleware,
  async (req, res) => {
    try {
      const member = await Member.findOne({
        phoneNumber: req.user.phoneNumber,
      });
      const memberId = member?._id;
      const userId = req.user._id;
      // Push both IDs to readBy to handle all possible stored ID variants
      const idsToAdd = [userId];
      if (memberId && memberId.toString() !== userId.toString()) {
        idsToAdd.push(memberId);
      }
      await FollowUpChat.updateMany(
        { assignmentId: req.params.assignmentId },
        { $addToSet: { readBy: { $each: idsToAdd } } },
      );
      res.json({ ok: true });
    } catch (e) {
      res.status(500).json({ error: "Server error" });
    }
  },
);

// GET /api/followup-chat/unread-count  — total unread messages across all threads for current user
app.get("/api/followup-chat/unread/count", authMiddleware, async (req, res) => {
  try {
    const member = await Member.findOne({ phoneNumber: req.user.phoneNumber });
    const memberId = member?._id;
    const userId = req.user._id;
    const userName = req.user.fullName;

    // Count messages unread by this user: match by toMemberId (both IDs) OR by toName
    const idFilter = [];
    if (memberId) idFilter.push(memberId);
    if (userId) idFilter.push(userId);

    const count = await FollowUpChat.countDocuments({
      $or: [
        { toMemberId: { $in: idFilter }, readBy: { $nin: idFilter } },
        { toName: userName, fromName: { $ne: userName } },
      ],
    });

    // Deduplicate: subtract messages already read by any of our IDs
    // (The simple count above may overcount — use a safer aggregate)
    const allUnread = await FollowUpChat.find({
      $or: [
        { toMemberId: { $in: idFilter } },
        { toName: userName, fromName: { $ne: userName } },
      ],
    }).lean();

    const actualUnread = allUnread.filter((m) => {
      const readByStrs = (m.readBy || []).map((id) => id.toString());
      return (
        !readByStrs.includes(memberId?.toString()) &&
        !readByStrs.includes(userId?.toString())
      );
    }).length;

    res.json({ count: actualUnread });
  } catch (e) {
    res.status(500).json({ error: "Server error" });
  }
});

// ═══════════════════════════════════════════════════════════════
// COMPREHENSIVE REPORT ENDPOINT
// GET /api/comprehensive-report?branch=...&quarter=Q1|Q2|Q3|Q4|all&year=2024
// ═══════════════════════════════════════════════════════════════
app.get(
  "/api/comprehensive-report",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Branch Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { branch, quarter } = req.query;
      // Support year param so past years can be reviewed; default to current year
      const year = parseInt(req.query.year, 10) || new Date().getFullYear();

      // ── Quarter date range ──
      const qDefs = {
        Q1: {
          start: new Date(year, 0, 1),
          end: new Date(year, 3, 0, 23, 59, 59, 999),
          label: `Q1 ${year} (Jan – Mar)`,
        },
        Q2: {
          start: new Date(year, 3, 1),
          end: new Date(year, 6, 0, 23, 59, 59, 999),
          label: `Q2 ${year} (Apr – Jun)`,
        },
        Q3: {
          start: new Date(year, 6, 1),
          end: new Date(year, 9, 0, 23, 59, 59, 999),
          label: `Q3 ${year} (Jul – Sep)`,
        },
        Q4: {
          start: new Date(year, 9, 1),
          end: new Date(year + 1, 0, 0, 23, 59, 59, 999),
          label: `Q4 ${year} (Oct – Dec)`,
        },
        all: {
          start: new Date(year, 0, 1),
          end: new Date(year, 11, 31, 23, 59, 59, 999),
          label: `All Quarters ${year}`,
        },
      };
      let dateFilter = {};
      let quarterLabel = `All Quarters ${year}`;
      // When a specific year is selected, always scope to that year
      if (quarter && quarter !== "all" && qDefs[quarter]) {
        dateFilter.date = {
          $gte: qDefs[quarter].start,
          $lte: qDefs[quarter].end,
        };
        quarterLabel = qDefs[quarter].label;
      } else {
        // Scope to the selected year for both all-quarters and year-only views
        dateFilter.date = {
          $gte: qDefs.all.start,
          $lte: qDefs.all.end,
        };
        quarterLabel = qDefs.all.label;
      }

      // ── Branch scope ──
      const targetBranch = branch && branch !== "all" ? branch : null;
      let branchGroupNames = [];
      if (targetBranch) {
        branchGroupNames = await Group.distinct("name", {
          branch: targetBranch,
        });
      }

      // ── Fetch attendance records ──
      const baseQuery = { ...dateFilter };
      if (targetBranch) {
        baseQuery.$or = [
          { branch: targetBranch },
          { group: { $in: branchGroupNames } },
        ];
      }
      const allAtt = await Attendance.find(baseQuery).lean();

      const byType = {
        fellowship: allAtt.filter((a) => a.type === "fellowship"),
        cbs: allAtt.filter((a) => a.type === "cbs"),
        evangelism: allAtt.filter((a) => a.type === "evangelism"),
      };

      // ── Helpers ──
      const pct = (p, t) =>
        t > 0 ? parseFloat(((p / t) * 100).toFixed(1)) : 0;

      // Build per-member stats from a set of sessions
      // memberBranch enrichment: each session carries branch + group so we can attach it
      const buildMemberStats = (sessions) => {
        const map = {};
        sessions.forEach((s) => {
          (s.records || []).forEach((r) => {
            const key = r.memberId?.toString() || r.memberName;
            if (!key) return;
            if (!map[key]) {
              map[key] = {
                id: key,
                name: r.memberName,
                present: 0,
                absent: 0,
                group: s.group || null,
                branch: s.branch || null,
              };
            }
            r.status === "present" ? map[key].present++ : map[key].absent++;
          });
        });
        return Object.values(map);
      };

      // Build per-group attendance stats from sessions
      const buildGroupStats = (sessions) => {
        const map = {};
        sessions.forEach((s) => {
          const g = s.group || "__unassigned__";
          if (!map[g]) map[g] = { present: 0, total: 0 };
          (s.records || []).forEach((r) => {
            map[g].total++;
            if (r.status === "present") map[g].present++;
          });
        });
        return map;
      };

      // Pick ALL winners (ties allowed): zero-absent first, else fewest absent
      // Returns an array of winners (could be multiple on tie)
      const findBestMembers = (sessions) => {
        const members = buildMemberStats(sessions).filter((m) => m.present > 0);
        if (!members.length) return [];
        const perfect = members.filter((m) => m.absent === 0);
        if (perfect.length) {
          const maxPresent = Math.max(...perfect.map((m) => m.present));
          return perfect
            .filter((m) => m.present === maxPresent)
            .map((m) => ({ ...m, perfect: true }));
        }
        const minAbsent = Math.min(...members.map((m) => m.absent));
        const nearPerfect = members.filter((m) => m.absent === minAbsent);
        const maxPresent = Math.max(...nearPerfect.map((m) => m.present));
        return nearPerfect
          .filter((m) => m.present === maxPresent)
          .map((m) => ({ ...m, perfect: false }));
      };

      // ── Group stats per activity type ──
      const fGS = buildGroupStats(byType.fellowship);
      const cGS = buildGroupStats(byType.cbs);
      const eGS = buildGroupStats(byType.evangelism);
      const allGroupNames = new Set([
        ...Object.keys(fGS),
        ...Object.keys(cGS),
        ...Object.keys(eGS),
      ]);
      allGroupNames.delete("__unassigned__");

      // Full group leaderboard with composite + per-type pcts
      const groupLeaderboard = [];
      allGroupNames.forEach((g) => {
        const f = fGS[g] || { present: 0, total: 0 };
        const c = cGS[g] || { present: 0, total: 0 };
        const e = eGS[g] || { present: 0, total: 0 };
        const fP = pct(f.present, f.total);
        const cP = pct(c.present, c.total);
        const eP = pct(e.present, e.total);
        const activeSections =
          (f.total > 0 ? 1 : 0) + (c.total > 0 ? 1 : 0) + (e.total > 0 ? 1 : 0);
        if (!activeSections) return;
        // Simple average: (Fellowship% + CBS% + Evangelism%) / 3
        const composite = parseFloat(((fP + cP + eP) / 3).toFixed(1));
        groupLeaderboard.push({
          name: g,
          composite,
          fellowship: { ...f, pct: fP },
          cbs: { ...c, pct: cP },
          evangelism: { ...e, pct: eP },
        });
      });
      groupLeaderboard.sort((a, b) => b.composite - a.composite);

      // ── 1. Best Group/s (overall composite) — supports ties ──
      const bestGroups = (() => {
        if (!groupLeaderboard.length) return [];
        const topScore = groupLeaderboard[0].composite;
        return groupLeaderboard.filter((g) => g.composite === topScore);
      })();

      // ── 1a. Best Group/s by Fellowship only ──
      const fellowshipGroupBoard = [...groupLeaderboard]
        .filter((g) => g.fellowship.total > 0)
        .sort((a, b) => b.fellowship.pct - a.fellowship.pct);
      const bestFellowshipGroups = (() => {
        if (!fellowshipGroupBoard.length) return [];
        const top = fellowshipGroupBoard[0].fellowship.pct;
        return fellowshipGroupBoard.filter((g) => g.fellowship.pct === top);
      })();

      // ── 1b. Best Group/s by CBS only ──
      const cbsGroupBoard = [...groupLeaderboard]
        .filter((g) => g.cbs.total > 0)
        .sort((a, b) => b.cbs.pct - a.cbs.pct);
      const bestCBSGroups = (() => {
        if (!cbsGroupBoard.length) return [];
        const top = cbsGroupBoard[0].cbs.pct;
        return cbsGroupBoard.filter((g) => g.cbs.pct === top);
      })();

      // ── 1c. Best Group/s by Evangelism only ──
      const evangelismGroupBoard = [...groupLeaderboard]
        .filter((g) => g.evangelism.total > 0)
        .sort((a, b) => b.evangelism.pct - a.evangelism.pct);
      const bestEvangelismGroups = (() => {
        if (!evangelismGroupBoard.length) return [];
        const top = evangelismGroupBoard[0].evangelism.pct;
        return evangelismGroupBoard.filter((g) => g.evangelism.pct === top);
      })();

      // ── 2–4. Best individual member/s per activity (multi-winner, cross-group) ──
      const bestFellowshipMembers = findBestMembers(byType.fellowship);
      const bestCBSMembers = findBestMembers(byType.cbs);
      const bestEvangelismMembers = findBestMembers(byType.evangelism);

      // ── 5. Best Overall Member/s (no absences across all three combined) ──
      const allSessions = [
        ...byType.fellowship,
        ...byType.cbs,
        ...byType.evangelism,
      ];
      const bestOverallMembers = findBestMembers(allSessions);

      // ── 6. Best Group Leader/s (own attendance + group performance) ──
      const leaderQuery = { role: "Group Leader" };
      if (targetBranch) leaderQuery.branch = targetBranch;
      const leaders = await User.find(leaderQuery).lean();
      const leaderLeaderboard = [];

      for (const leader of leaders) {
        const leaderMember = await Member.findOne({
          phoneNumber: leader.phoneNumber,
        }).lean();
        let ownPresent = 0,
          ownTotal = 0;
        if (leaderMember) {
          allSessions.forEach((s) => {
            const r = (s.records || []).find(
              (rec) => rec.memberId?.toString() === leaderMember._id.toString(),
            );
            if (r) {
              ownTotal++;
              if (r.status === "present") ownPresent++;
            }
          });
        }
        const g = leader.group;
        let groupComposite = 0;
        if (g) {
          const gf = fGS[g] || { present: 0, total: 0 };
          const gc = cGS[g] || { present: 0, total: 0 };
          const ge = eGS[g] || { present: 0, total: 0 };
          // Simple average: (Fellowship% + CBS% + Evangelism%) / 3
          groupComposite = parseFloat(
            (
              (pct(gf.present, gf.total) +
                pct(gc.present, gc.total) +
                pct(ge.present, ge.total)) /
              3
            ).toFixed(1),
          );
        }
        const ownPct = pct(ownPresent, ownTotal);
        // Leader score: average of own attendance % and group composite %
        const composite = parseFloat(
          (ownTotal > 0
            ? (ownPct + groupComposite) / 2
            : groupComposite
          ).toFixed(1),
        );
        leaderLeaderboard.push({
          name: leader.fullName,
          group: g,
          ownPct,
          ownPresent,
          ownTotal,
          groupPct: groupComposite,
          composite,
        });
      }
      leaderLeaderboard.sort((a, b) => b.composite - a.composite);

      // Multi-winner leaders
      const bestLeaders = (() => {
        if (!leaderLeaderboard.length) return [];
        const topScore = leaderLeaderboard[0].composite;
        return leaderLeaderboard.filter((l) => l.composite === topScore);
      })();

      // ── 7. Best CBS Location/s ──
      const cbsLocMap = {};
      byType.cbs.forEach((s) => {
        const loc = s.cbsLocation || "__none__";
        if (loc === "__none__") return;
        if (!cbsLocMap[loc])
          cbsLocMap[loc] = { present: 0, total: 0, sessions: 0 };
        cbsLocMap[loc].sessions++;
        (s.records || []).forEach((r) => {
          cbsLocMap[loc].total++;
          if (r.status === "present") cbsLocMap[loc].present++;
        });
      });
      const cbsLocLeaderboard = Object.entries(cbsLocMap)
        .map(([name, s]) => ({ name, ...s, pct: pct(s.present, s.total) }))
        .sort((a, b) => b.pct - a.pct);

      const bestCBSLocations = (() => {
        if (!cbsLocLeaderboard.length) return [];
        const topPct = cbsLocLeaderboard[0].pct;
        return cbsLocLeaderboard.filter((l) => l.pct === topPct);
      })();

      res.json({
        branch: targetBranch || "All Branches",
        quarter: quarter || "all",
        quarterLabel,
        generatedAt: new Date().toISOString(),
        stats: {
          totalFellowshipSessions: byType.fellowship.length,
          totalCBSSessions: byType.cbs.length,
          totalEvangelismSessions: byType.evangelism.length,
          totalSessions: allAtt.length,
        },
        // Group awards
        bestGroups, // overall composite
        bestFellowshipGroups, // fellowship only
        bestCBSGroups, // cbs only
        bestEvangelismGroups, // evangelism only
        groupLeaderboard: groupLeaderboard.slice(0, 10),
        fellowshipGroupBoard: fellowshipGroupBoard.slice(0, 10),
        cbsGroupBoard: cbsGroupBoard.slice(0, 10),
        evangelismGroupBoard: evangelismGroupBoard.slice(0, 10),
        // Member awards (arrays — ties supported, cross-group)
        bestFellowshipMembers,
        bestCBSMembers,
        bestEvangelismMembers,
        bestOverallMembers,
        // Leader awards (arrays — ties supported)
        bestLeaders,
        leaderLeaderboard: leaderLeaderboard.slice(0, 10),
        // CBS Location awards (arrays — ties supported)
        bestCBSLocations,
        cbsLocLeaderboard: cbsLocLeaderboard.slice(0, 10),
      });
    } catch (err) {
      console.error("Comprehensive report error:", err);
      res.status(500).json({ error: err.message || "Server error" });
    }
  },
);

const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🚀 MOR System Backend Server v2.0`);
  console.log(`📡 Running on http://0.0.0.0:${PORT}`);
  console.log(`🔗 API: https://mor-system-backend.onrender.com/api`);
});

module.exports = app;
