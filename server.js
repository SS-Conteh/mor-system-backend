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
  "Discipleship",
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
  discipleshipCount: { type: Number, default: 0 },
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
    const cbsOnly = req.query.cbsOnly === "true";
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
    await logActivity(`updated member ${member.fullName}`, req.user);
    res.json(updatedMember);
  } catch (error) {
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
      if (!group) query.group = req.user.group;
    } else if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      if (!branch) query.branch = req.user.branch;
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
app.get("/api/cbs-locations", authMiddleware, async (req, res) => {
  try {
    const query = {};
    if (req.query.branch) query.branch = req.query.branch;
    else if (req.user.role === "Branch Head Shepherd")
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
      const effectiveGroup = group || req.user.group;

      // ── Check for an existing session for the same group+type+date ──────
      const dateStr = sessionDate.toISOString().split("T")[0];
      const existingQuery =
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
      const existing = await QRSession.findOne(existingQuery);
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

    // Add scan to QRSession
    session.scans.push({
      memberId: member._id,
      memberName: member.fullName,
      group: member.group,
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
        branch: session.branch || member.branch || null,
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
    if (req.user.role === "Group Leader") query.group = req.user.group;
    else if (req.user.role === "Branch Head Shepherd")
      query.branch = req.user.branch;
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
    else if (req.user.role === "Member") query.assignedTo = req.user._id;
    if (req.query.assignedTo) query.assignedTo = req.query.assignedTo;
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
    // Branch Head Shepherd only sees activity from their own branch
    if (req.user.role === "Branch Head Shepherd" && req.user.branch) {
      query.branch = req.user.branch;
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
    const groups = [
      "General",
      "Success",
      "Empowerment",
      "Zoe",
      "Favour",
      "Dominion",
    ];
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
    const locations = [
      "Brookfields",
      "Tengbeh Town",
      "Leicester",
      "Regent",
      "Kissy Road",
      "Kissy Shell",
      "Goderich",
      "Murray Town",
      "Saint John",
      "Circular Road",
      "Tree Planting",
    ];
    for (const location of locations) {
      if (!(await CBSLocation.findOne({ name: location }))) {
        await CBSLocation.create({
          name: location,
          branch: "MOR Head Quarter",
          status: "Active",
          memberCount: 0,
          associatedGroups: groups,
        });
        console.log(`   ✓ Created CBS location: ${location}`);
      }
    }
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

      // Resolve sender/receiver from the assignment
      const senderMember = await Member.findOne({
        phoneNumber: req.user.phoneNumber,
      });
      const senderId = senderMember?._id || req.user._id;
      const senderName = req.user.fullName;
      const senderRole =
        req.user.role === "Member"
          ? "Member"
          : req.user.role.includes("Leader")
            ? "Leader"
            : "Steward";

      // Determine the other party
      let toMemberId, toName;
      if (req.user.role === "Member") {
        // member → their leader/steward
        toMemberId = assignment.assignedTo;
        toName = assignment.assignedToName;
      } else {
        // leader/steward → the assigned member
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
      const userId = member?._id || req.user._id;
      await FollowUpChat.updateMany(
        { assignmentId: req.params.assignmentId, readBy: { $ne: userId } },
        { $push: { readBy: userId } },
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
    const userId = member?._id || req.user._id;
    const count = await FollowUpChat.countDocuments({
      toMemberId: userId,
      readBy: { $ne: userId },
    });
    res.json({ count });
  } catch (e) {
    res.status(500).json({ error: "Server error" });
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🚀 MOR System Backend Server v2.0`);
  console.log(`📡 Running on http://0.0.0.0:${PORT}`);
  console.log(`🔗 API: https://mor-system-backend.onrender.com/api`);
});

module.exports = app;
