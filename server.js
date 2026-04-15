require("dotenv").config();
const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const path = require("path");
const multer = require("multer");
const fs = require("fs");

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
    ],
    credentials: true,
  }),
);
app.use(express.json());
app.use(express.static(path.join(__dirname, "../frontend")));

// ========== DATABASE MODELS ==========

// User Model
const UserSchema = new mongoose.Schema({
  fullName: { type: String, required: true },
  phoneNumber: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  role: {
    type: String,
    enum: ["Head Shepherd", "Group Leader", "Member", "System Admin"],
    default: "Member",
  },
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
    enum: [
      "First Timer",
      "Inconsistent",
      "Semi-Consistent",
      "Consistent",
      "Potential Leader",
      "Intense Leader",
      "Discipleship",
      "Leader",
    ],
    default: "First Timer",
  },
  isSteward: { type: Boolean, default: false },
  stewardSince: Date,
  isCBSLeader: { type: Boolean, default: false },
  assignedCBSLocation: { type: String, default: null },
  isGroupLeader: { type: Boolean, default: false },
  isHeadShepherd: { type: Boolean, default: false },
  isSystemAdmin: { type: Boolean, default: false },
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
  group: { type: String, default: null },
  cbsLocation: String,
  membershipStatus: {
    type: String,
    enum: [
      "First Timer",
      "Inconsistent",
      "Semi-Consistent",
      "Consistent",
      "Potential Leader",
      "Intense Leader",
      "Discipleship",
      "Leader",
    ],
    default: "First Timer",
  },
  isSteward: { type: Boolean, default: false },
  stewardSince: Date,
  isCBSLeader: { type: Boolean, default: false },
  assignedCBSLocation: { type: String, default: null },
  isGroupLeader: { type: Boolean, default: false },
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
  group: String,
  cbsLocation: String,
  date: { type: Date, required: true },
  records: [
    {
      memberId: { type: mongoose.Schema.Types.ObjectId, ref: "Member" },
      memberName: String,
      status: { type: String, enum: ["present", "absent"], default: "absent" },
      checkInTime: Date,
    },
  ],
  stats: { total: Number, present: Number, absent: Number, percentage: Number },
  recordedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  recordedByName: String,
  createdAt: { type: Date, default: Date.now },
});

// Group Model
const GroupSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    unique: true,
  },
  leader: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  leaderName: String,
  leaderPhone: String,
  assistantLeader: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  assistantLeaderName: String,
  assistantLeaderPhone: String,
  memberCount: { type: Number, default: 0 },
  stewardCount: { type: Number, default: 0 },
  intenseLeaderCount: { type: Number, default: 0 },
  potentialLeaderCount: { type: Number, default: 0 },
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

// Notification Model
const NotificationSchema = new mongoose.Schema({
  title: { type: String, required: true },
  message: { type: String, required: true },
  type: {
    type: String,
    enum: ["general", "group", "personal"],
    default: "general",
  },
  targetGroup: String,
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
const Notification = mongoose.model("Notification", NotificationSchema);
const ActivityLog = mongoose.model("ActivityLog", ActivityLogSchema);
const Media = mongoose.model("Media", MediaSchema);

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

const roleMiddleware = (...roles) => {
  return (req, res, next) => {
    if (!roles.includes(req.user.role))
      return res.status(403).json({ error: "Access denied" });
    next();
  };
};

// ========== MULTER CONFIGURATION (after authMiddleware) ==========

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    const uploadDir = path.join(__dirname, "../frontend/uploads");
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    cb(null, "profile-" + uniqueSuffix + path.extname(file.originalname));
  },
});

const upload = multer({
  storage: storage,
  limits: { fileSize: 5 * 1024 * 1024 }, // 5MB limit
  fileFilter: (req, file, cb) => {
    const allowedTypes = /jpeg|jpg|png|gif/;
    const extname = allowedTypes.test(
      path.extname(file.originalname).toLowerCase(),
    );
    const mimetype = allowedTypes.test(file.mimetype);
    if (mimetype && extname) {
      return cb(null, true);
    } else {
      cb(new Error("Only image files are allowed"));
    }
  },
});

// Serve uploaded files statically
app.use(
  "/uploads",
  express.static(path.join(__dirname, "../frontend/uploads")),
);

// ========== DATABASE CONNECTION ==========
console.log("🔌 Connecting to MongoDB Atlas...");

mongoose
  .connect(process.env.MONGODB_URI)
  .then(() => {
    console.log("✅ Successfully connected to MongoDB Atlas!");
    initializeDatabase();
  })
  .catch((err) => {
    console.error("❌ MongoDB connection error:", err.message);
    process.exit(1);
  });

// ========== API ROUTES ==========

// Serve role-specific pages
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "../frontend/index.html"));
});
app.get("/group-leader", (req, res) => {
  res.sendFile(path.join(__dirname, "../frontend/group-leader.html"));
});
app.get("/member", (req, res) => {
  res.sendFile(path.join(__dirname, "../frontend/member.html"));
});

// API Info
app.get("/api", (req, res) => {
  res.json({ name: "MOR System API", version: "1.0.0", status: "running" });
});

// Health Check
app.get("/api/health", (req, res) => {
  res.json({
    status: "OK",
    timestamp: new Date(),
    database:
      mongoose.connection.readyState === 1 ? "Connected" : "Disconnected",
  });
});

// ========== ACTIVITY LOG HELPER ==========
async function logActivity(action, user, details = "") {
  try {
    await ActivityLog.create({
      action,
      user: user?._id,
      userName: user?.fullName || "System",
      userRole: user?.role || "System",
      details,
    });
  } catch (e) {
    /* non-critical */
  }
}

// ========== AUTH ROUTES ==========

// Register a member as a full user+member account (by Head Shepherd/Group Leader)
app.post(
  "/api/auth/register-member",
  authMiddleware,
  roleMiddleware("Head Shepherd", "Group Leader", "System Admin"),
  async (req, res) => {
    try {
      const {
        fullName,
        phoneNumber,
        password,
        group,
        membershipStatus,
        ...otherFields
      } = req.body;
      if (!fullName || !phoneNumber || !password)
        return res
          .status(400)
          .json({ error: "fullName, phoneNumber and password are required" });

      const existingUser = await User.findOne({ phoneNumber });
      if (existingUser)
        return res
          .status(400)
          .json({ error: "Phone number already registered" });

      const hashedPassword = await bcrypt.hash(password, 10);
      const user = new User({
        fullName,
        phoneNumber,
        password: hashedPassword,
        role: "Member",
        group: group || null,
        membershipStatus: membershipStatus || "First Timer",
        ...otherFields,
      });
      await user.save();

      // Check if member record already exists (by phone)
      let member = await Member.findOne({ phoneNumber });
      if (!member) {
        member = new Member({
          fullName,
          phoneNumber,
          group: group || null,
          membershipStatus: membershipStatus || "First Timer",
          ...otherFields,
          addedBy: req.user._id,
          addedByName: req.user.fullName,
        });
        await member.save();
      }

      if (member.group && member.group !== "General") {
        await Group.findOneAndUpdate(
          { name: member.group },
          { $inc: { memberCount: 1 } },
        );
      }

      await logActivity(
        `added new member ${fullName} with account access`,
        req.user,
      );
      res
        .status(201)
        .json({ message: "Member registered successfully", member });
    } catch (error) {
      console.error("Register member error:", error);
      res.status(500).json({ error: error.message || "Server error" });
    }
  },
);

app.post("/api/auth/register", async (req, res) => {
  try {
    const { fullName, phoneNumber, password, group, ...otherFields } = req.body;
    const existingUser = await User.findOne({ phoneNumber });
    if (existingUser)
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
      group: group || null,
      ...otherFields,
    });
    await user.save();

    const member = new Member({
      fullName,
      phoneNumber,
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
      },
    });
  } catch (error) {
    console.error("Registration error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const { phoneNumber, password } = req.body;
    const user = await User.findOne({ phoneNumber });
    if (!user) return res.status(401).json({ error: "Invalid credentials" });

    const isValid = await bcrypt.compare(password, user.password);
    if (!isValid) return res.status(401).json({ error: "Invalid credentials" });

    const token = jwt.sign(
      { userId: user._id, role: user.role },
      process.env.JWT_SECRET,
      { expiresIn: "7d" },
    );
    user.lastLogin = new Date();
    await user.save();
    await logActivity(`logged in`, user);

    res.json({
      message: "Login successful",
      token,
      user: {
        id: user._id,
        fullName: user.fullName,
        phoneNumber: user.phoneNumber,
        role: user.role,
        group: user.group,
        isSteward: user.isSteward,
        membershipStatus: user.membershipStatus,
      },
    });
  } catch (error) {
    console.error("Login error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.get("/api/auth/verify", authMiddleware, async (req, res) => {
  try {
    res.json({
      valid: true,
      user: {
        id: req.user._id,
        role: req.user.role,
        fullName: req.user.fullName,
      },
    });
  } catch (error) {
    res.status(401).json({ valid: false, error: "Invalid token" });
  }
});

// ========== PROFILE PHOTO UPLOAD ROUTE ==========

app.post(
  "/api/profile/photo",
  authMiddleware,
  upload.single("photo"),
  async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: "No file uploaded" });
      }

      const photoUrl = `/uploads/${req.file.filename}`;

      // Update User
      await User.findByIdAndUpdate(req.user._id, { profilePhoto: photoUrl });

      // Update Member
      await Member.findOneAndUpdate(
        { phoneNumber: req.user.phoneNumber },
        { profilePhoto: photoUrl },
      );

      res.json({ photoUrl, message: "Profile photo updated successfully" });
    } catch (error) {
      console.error("Upload photo error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== MEMBER ROUTES ==========

app.get("/api/members", authMiddleware, async (req, res) => {
  try {
    const { group, status, search, steward, cbsLocation } = req.query;
    let query = {};
    if (req.user.role === "Group Leader" && req.user.group)
      query.group = req.user.group;
    if (group && group !== "All Groups") query.group = group;
    if (status && status !== "All Statuses") query.membershipStatus = status;
    if (steward === "true") query.isSteward = true;
    if (cbsLocation && cbsLocation !== "All Locations")
      query.cbsLocation = cbsLocation;
    if (search)
      query.$or = [
        { fullName: { $regex: search, $options: "i" } },
        { phoneNumber: { $regex: search, $options: "i" } },
      ];

    const members = await Member.find(query).sort({ createdAt: -1 });

    // Enrich each member with profilePhoto from the User collection
    // (existing Member documents may not have profilePhoto stored directly)
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
      // Use member's own profilePhoto if set, otherwise fall back to user's photo
      if (!obj.profilePhoto && userPhotoMap[m.phoneNumber]) {
        obj.profilePhoto = userPhotoMap[m.phoneNumber];
      }
      return obj;
    });

    res.json(enriched);
  } catch (error) {
    console.error("Get members error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/members", authMiddleware, async (req, res) => {
  try {
    if (req.user.role === "Group Leader" && req.body.group !== req.user.group) {
      return res
        .status(403)
        .json({ error: "You can only add members to your own group" });
    }
    const member = new Member({
      ...req.body,
      addedBy: req.user._id,
      addedByName: req.user.fullName,
    });
    await member.save();

    if (member.group) {
      await Group.findOneAndUpdate(
        { name: member.group },
        { $inc: { memberCount: 1 } },
      );
    }
    res.status(201).json(member);
  } catch (error) {
    console.error("Add member error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.put("/api/members/:id", authMiddleware, async (req, res) => {
  try {
    const member = await Member.findById(req.params.id);
    if (!member) return res.status(404).json({ error: "Member not found" });
    if (req.user.role === "Group Leader" && member.group !== req.user.group) {
      return res
        .status(403)
        .json({ error: "You can only update members in your own group" });
    }
    const updatedMember = await Member.findByIdAndUpdate(
      req.params.id,
      { ...req.body, updatedAt: new Date() },
      { new: true, runValidators: false },
    );
    // If profilePhoto was updated, sync it to User record as well
    if (req.body.profilePhoto) {
      await User.findOneAndUpdate(
        { phoneNumber: member.phoneNumber },
        { profilePhoto: req.body.profilePhoto },
        { runValidators: false },
      );
    }
    await logActivity(`updated member ${member.fullName}`, req.user);
    res.json(updatedMember);
  } catch (error) {
    console.error("Update member error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// DELETE member — removes member record, user account, and attendance records
app.delete(
  "/api/members/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin", "Group Leader"),
  async (req, res) => {
    try {
      const member = await Member.findById(req.params.id);
      if (!member) return res.status(404).json({ error: "Member not found" });
      if (req.user.role === "Group Leader" && member.group !== req.user.group) {
        return res
          .status(403)
          .json({ error: "You can only delete members in your own group" });
      }
      const memberName = member.fullName;
      const phoneNumber = member.phoneNumber;

      // 1. Delete member record
      await Member.findByIdAndDelete(req.params.id);

      // 2. Delete user account if exists
      await User.findOneAndDelete({ phoneNumber });

      // 3. Remove member from all attendance records
      await Attendance.updateMany(
        { "records.memberName": memberName },
        { $pull: { records: { memberName: memberName } } },
      );

      // 4. Decrement group count
      if (member.group) {
        await Group.findOneAndUpdate(
          { name: member.group },
          { $inc: { memberCount: -1 } },
        );
      }

      await logActivity(
        `deleted member ${memberName} and all their records`,
        req.user,
      );
      res.json({
        message: `Member ${memberName} and all records deleted successfully`,
      });
    } catch (error) {
      console.error("Delete member error:", error);
      res.status(500).json({ error: error.message || "Server error" });
    }
  },
);

// Sync profilePhoto from User → Member for all existing members (back-fill)
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
      console.error("Sync photos error:", error);
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
      if (req.user.role === "Group Leader" && member.group !== req.user.group) {
        return res
          .status(403)
          .json({ error: "You can only manage stewards in your own group" });
      }
      member.isSteward = !member.isSteward;
      if (member.isSteward) {
        member.stewardSince = new Date();
      } else {
        member.stewardSince = undefined;
      }
      await member.save();

      // Only update group stewardCount if member has a group
      if (member.group) {
        const stewardCount = await Member.countDocuments({
          group: member.group,
          isSteward: true,
        });
        await Group.findOneAndUpdate(
          { name: member.group },
          { stewardCount: stewardCount },
        );
      }
      res.json(member);
    } catch (error) {
      console.error("Toggle steward error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== ATTENDANCE ROUTES (FIXED + CBS SUPPORT) ==========
app.get("/api/attendance", authMiddleware, async (req, res) => {
  try {
    const { type, group, cbsLocation, date } = req.query;
    let query = {};

    if (type) query.type = type;
    if (group) query.group = group;
    if (cbsLocation) query.cbsLocation = cbsLocation;

    if (date) {
      const start = new Date(date);
      start.setHours(0, 0, 0, 0);
      const end = new Date(start);
      end.setDate(end.getDate() + 1);
      query.date = { $gte: start, $lt: end };
    }

    const records = await Attendance.find(query).sort({ date: -1 }).lean();

    res.json(records);
  } catch (error) {
    console.error("Get attendance error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/attendance", authMiddleware, async (req, res) => {
  try {
    const { type, group, cbsLocation, date, records } = req.body;

    if (!type || !date || !records) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    const total = records.length;
    const present = records.filter((r) => r.status === "present").length;

    const attendance = new Attendance({
      type,
      group: type === "cbs" ? undefined : group,
      cbsLocation: type === "cbs" ? cbsLocation : undefined,
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
    console.error("Save attendance error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.get("/api/attendance", authMiddleware, async (req, res) => {
  try {
    const { type, group, cbsLocation, date, limit = 50 } = req.query;
    let query = {};
    if (type) query.type = type;
    if (group) query.group = group;
    if (cbsLocation) query.cbsLocation = cbsLocation;
    if (date) {
      const d = new Date(date);
      const next = new Date(d);
      next.setDate(next.getDate() + 1);
      query.date = { $gte: d, $lt: next };
    }
    if (req.user.role === "Member") {
      const member = await Member.findOne({
        phoneNumber: req.user.phoneNumber,
      });
      if (member) query["records.memberId"] = member._id;
    } else if (req.user.role === "Group Leader" && req.user.group) {
      query.group = req.user.group;
    }
    const attendance = await Attendance.find(query)
      .sort({ date: -1 })
      .limit(parseInt(limit));
    res.json(attendance);
  } catch (error) {
    console.error("Get attendance error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// Update existing attendance record (upsert — prevents duplicate date entries)
app.put("/api/attendance/:id", authMiddleware, async (req, res) => {
  try {
    const { records } = req.body;
    const attendance = await Attendance.findById(req.params.id);
    if (!attendance)
      return res.status(404).json({ error: "Attendance record not found" });

    attendance.records = records;
    const total = records.length;
    const present = records.filter((r) => r.status === "present").length;
    const absent = total - present;
    const percentage = total > 0 ? ((present / total) * 100).toFixed(1) : 0;
    attendance.stats = { total, present, absent, percentage };
    attendance.updatedAt = new Date();
    await attendance.save();
    await logActivity(
      `updated ${attendance.type} attendance for ${attendance.group || attendance.cbsLocation || ""} on ${new Date(attendance.date).toLocaleDateString()}`,
      req.user,
    );
    res.json(attendance);
  } catch (error) {
    console.error("Update attendance error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// ========== DASHBOARD ROUTES ==========

app.get("/api/dashboard/stats", authMiddleware, async (req, res) => {
  try {
    let totalMembers = await Member.countDocuments();
    let totalStewards = await Member.countDocuments({ isSteward: true });

    if (req.user.role === "Group Leader" && req.user.group) {
      totalMembers = await Member.countDocuments({ group: req.user.group });
      totalStewards = await Member.countDocuments({
        group: req.user.group,
        isSteward: true,
      });
    } else if (req.user.role === "Member") {
      totalMembers = 1;
      totalStewards = 0;
    }

    const groups = await Group.find();
    const groupPerformance = [];
    for (const group of groups) {
      const groupMembers = await Member.find({ group: group.name });
      groupPerformance.push({
        name: group.name,
        memberCount: groupMembers.length,
        stewardCount: groupMembers.filter((m) => m.isSteward).length,
        intenseLeaderCount: groupMembers.filter(
          (m) => m.membershipStatus === "Intense Leader",
        ).length,
      });
    }

    res.json({
      totalMembers,
      activeGroups: groups.length,
      totalStewards,
      totalCBSLocations: await CBSLocation.countDocuments(),
      groupPerformance,
    });
  } catch (error) {
    console.error("Dashboard stats error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// ========== GROUPS ROUTES ==========

app.get("/api/groups", authMiddleware, async (req, res) => {
  try {
    const groups = await Group.find();
    const members = await Member.find();
    const groupsWithCounts = groups.map((group) => ({
      ...group.toObject(),
      memberCount: members.filter((m) => m.group === group.name).length,
      stewardCount: members.filter((m) => m.group === group.name && m.isSteward)
        .length,
      intenseLeaderCount: members.filter(
        (m) =>
          m.group === group.name && m.membershipStatus === "Intense Leader",
      ).length,
      potentialLeaderCount: members.filter(
        (m) =>
          m.group === group.name && m.membershipStatus === "Potential Leader",
      ).length,
      consistentCount: members.filter(
        (m) => m.group === group.name && m.membershipStatus === "Consistent",
      ).length,
    }));
    res.json(groupsWithCounts);
  } catch (error) {
    console.error("Get groups error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// POST — create a new group
app.post(
  "/api/groups",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { name, description, leaderId } = req.body;
      if (!name)
        return res.status(400).json({ error: "Group name is required" });
      const existing = await Group.findOne({ name });
      if (existing)
        return res.status(400).json({ error: "Group already exists" });

      let leaderInfo = {};
      if (leaderId) {
        let leaderUser = await User.findById(leaderId);
        if (!leaderUser) {
          const m = await Member.findById(leaderId);
          if (m)
            leaderUser = await User.findOne({ phoneNumber: m.phoneNumber });
        }
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
        description: description || `${name} Ministry Group`,
        isActive: true,
        memberCount: 0,
        ...leaderInfo,
      });
      await group.save();
      await logActivity(`created group "${name}"`, req.user);
      res.status(201).json(group);
    } catch (error) {
      console.error("Create group error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

// DELETE — remove a group
app.delete(
  "/api/groups/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const group = await Group.findById(req.params.id);
      if (!group) return res.status(404).json({ error: "Group not found" });
      const groupName = group.name;
      await Group.findByIdAndDelete(req.params.id);
      await logActivity(`deleted group "${groupName}"`, req.user);
      res.json({ message: "Group deleted successfully" });
    } catch (error) {
      console.error("Delete group error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.put(
  "/api/groups/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { name, description, leaderId, assistantLeaderId } = req.body;
      const group = await Group.findById(req.params.id);
      if (!group) return res.status(404).json({ error: "Group not found" });

      if (name) group.name = name;
      if (description !== undefined) group.description = description;

      // Helper: resolve member/user ID to a User
      const resolveToUser = async (id) => {
        if (!id) return null;
        let u = await User.findById(id);
        if (!u) {
          const m = await Member.findById(id);
          if (m) u = await User.findOne({ phoneNumber: m.phoneNumber });
        }
        return u;
      };

      // Head Leader
      if (leaderId !== undefined) {
        const leaderUser = await resolveToUser(leaderId);
        if (leaderUser) {
          group.leader = leaderUser._id;
          group.leaderName = leaderUser.fullName;
          group.leaderPhone = leaderUser.phoneNumber;
          await User.findByIdAndUpdate(leaderUser._id, {
            isGroupLeader: true,
            role: "Group Leader",
          });
          await Member.findOneAndUpdate(
            { phoneNumber: leaderUser.phoneNumber },
            { isGroupLeader: true },
            { runValidators: false },
          );
        } else if (leaderId === "" || leaderId === null) {
          group.leader = null;
          group.leaderName = null;
          group.leaderPhone = null;
        }
      }

      // Assistant Head Leader
      if (assistantLeaderId !== undefined) {
        const assistantUser = await resolveToUser(assistantLeaderId);
        if (assistantUser) {
          group.assistantLeader = assistantUser._id;
          group.assistantLeaderName = assistantUser.fullName;
          group.assistantLeaderPhone = assistantUser.phoneNumber;
          await User.findByIdAndUpdate(assistantUser._id, {
            isGroupLeader: true,
            role: "Group Leader",
          });
          await Member.findOneAndUpdate(
            { phoneNumber: assistantUser.phoneNumber },
            { isGroupLeader: true },
            { runValidators: false },
          );
        } else if (assistantLeaderId === "" || assistantLeaderId === null) {
          group.assistantLeader = null;
          group.assistantLeaderName = null;
          group.assistantLeaderPhone = null;
        }
      }

      await group.save();
      await logActivity(`updated group "${group.name}"`, req.user);
      res.json(group);
    } catch (error) {
      console.error("Update group error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== CBS LOCATIONS ROUTES ==========

app.get("/api/cbs-locations", authMiddleware, async (req, res) => {
  try {
    const locations = await CBSLocation.find();
    res.json(locations);
  } catch (error) {
    console.error("Get CBS locations error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.post(
  "/api/cbs-locations",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { name, leaderId, associatedGroups } = req.body;

      const existing = await CBSLocation.findOne({ name });
      if (existing) {
        return res.status(400).json({ error: "CBS location already exists" });
      }

      let leaderInfo = {};
      if (leaderId) {
        const leader = await User.findById(leaderId);
        if (leader) {
          leaderInfo = {
            leader: leaderId,
            leaderName: leader.fullName,
            leaderPhone: leader.phoneNumber,
          };
        }
      }

      const location = new CBSLocation({
        name,
        associatedGroups: associatedGroups || [],
        status: "Active",
        ...leaderInfo,
      });

      await location.save();
      res.status(201).json(location);
    } catch (error) {
      console.error("Create CBS location error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.put("/api/cbs-locations/:id", authMiddleware, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, leaderId, status } = req.body;

    const location = await CBSLocation.findById(id);
    if (!location) {
      return res.status(404).json({ error: "CBS location not found" });
    }

    // Update fields
    if (name) location.name = name;
    if (status) location.status = status;

    // Update leader if provided
    if (leaderId) {
      // Try User first, then Member (frontend passes Member _id)
      let leaderUser = await User.findById(leaderId);
      if (!leaderUser) {
        const leaderMember = await Member.findById(leaderId);
        if (leaderMember) {
          leaderUser = await User.findOne({
            phoneNumber: leaderMember.phoneNumber,
          });
          // If no user account, use member info directly
          if (!leaderUser) {
            location.leader = leaderMember._id;
            location.leaderName = leaderMember.fullName;
            location.leaderPhone = leaderMember.phoneNumber;
            await location.save();
            return res.json(location);
          }
        }
      }
      if (leaderUser) {
        location.leader = leaderUser._id;
        location.leaderName = leaderUser.fullName;
        location.leaderPhone = leaderUser.phoneNumber;
        // Update user's CBS leader status
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
      // Remove leader
      location.leader = null;
      location.leaderName = null;
      location.leaderPhone = null;
    }

    await location.save();
    res.json(location);
  } catch (error) {
    console.error("Update CBS location error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.delete(
  "/api/cbs-locations/:id",
  authMiddleware,
  roleMiddleware("Head Shepherd", "System Admin"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const location = await CBSLocation.findByIdAndDelete(id);
      if (!location) {
        return res.status(404).json({ error: "CBS location not found" });
      }
      res.json({ message: "CBS location deleted successfully" });
    } catch (error) {
      console.error("Delete CBS location error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

// ========== NOTIFICATION ROUTES ==========

app.get("/api/notifications", authMiddleware, async (req, res) => {
  try {
    let query = {};
    if (req.user.role === "Member") {
      query = {
        $or: [
          { type: "general" },
          { type: "group", targetGroup: req.user.group },
          { type: "personal", targetUser: req.user._id },
        ],
      };
    } else if (req.user.role === "Group Leader") {
      query = {
        $or: [
          { type: "general" },
          { type: "group", targetGroup: req.user.group },
        ],
      };
    }
    const notifications = await Notification.find(query)
      .sort({ createdAt: -1 })
      .limit(50);
    const notificationsWithStatus = notifications.map((notif) => ({
      ...notif.toObject(),
      isRead: notif.readBy.includes(req.user._id),
    }));
    res.json(notificationsWithStatus);
  } catch (error) {
    console.error("Get notifications error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/notifications", authMiddleware, async (req, res) => {
  try {
    if (req.user.role === "Member") {
      req.body.type = "group";
      req.body.targetGroup = req.user.group;
    }
    const notification = new Notification({
      ...req.body,
      sentBy: req.user._id,
      sentByName: req.user.fullName,
      sentByRole: req.user.role,
    });
    await notification.save();
    const target =
      req.body.type === "general"
        ? "all members"
        : `${req.body.targetGroup || ""} group`;
    await logActivity(
      `sent notification "${notification.title}" to ${target}`,
      req.user,
    );
    res.status(201).json(notification);
  } catch (error) {
    console.error("Send notification error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/notifications/:id/read", authMiddleware, async (req, res) => {
  try {
    const notification = await Notification.findById(req.params.id);
    if (!notification)
      return res.status(404).json({ error: "Notification not found" });
    if (!notification.readBy.includes(req.user._id)) {
      notification.readBy.push(req.user._id);
      await notification.save();
    }
    res.json({ message: "Marked as read" });
  } catch (error) {
    console.error("Mark read error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.delete("/api/notifications/:id", authMiddleware, async (req, res) => {
  try {
    const notification = await Notification.findById(req.params.id);
    if (!notification)
      return res.status(404).json({ error: "Notification not found" });
    // Only sender or Head Shepherd/Admin can delete
    if (
      notification.sentBy.toString() !== req.user._id.toString() &&
      !["Head Shepherd", "System Admin"].includes(req.user.role)
    ) {
      return res
        .status(403)
        .json({ error: "You can only delete your own notifications" });
    }
    await Notification.findByIdAndDelete(req.params.id);
    res.json({ message: "Notification deleted" });
  } catch (error) {
    console.error("Delete notification error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// ========== PROFILE ROUTES ==========

app.get("/api/profile", authMiddleware, async (req, res) => {
  try {
    const user = await User.findById(req.user._id).select("-password");
    const member = await Member.findOne({ phoneNumber: req.user.phoneNumber });
    // Merge: member fields take priority for ministry data, user for role/auth data
    const profile = {
      ...(member ? member.toObject() : {}),
      fullName: user.fullName,
      phoneNumber: user.phoneNumber,
      role: user.role,
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
    };
    res.json(profile);
  } catch (error) {
    console.error("Get profile error:", error);
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
      group,
      phoneNumber,
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
    if (
      group &&
      [
        "General",
        "Success",
        "Empowerment",
        "Zoe",
        "Favour",
        "Dominion",
      ].includes(group)
    )
      user.group = group;

    await user.save();

    // Sync to Member record
    const memberUpdate = {};
    if (fullName) memberUpdate.fullName = fullName;
    if (address !== undefined) memberUpdate.address = address;
    if (occupation !== undefined) memberUpdate.occupation = occupation;
    if (school !== undefined) memberUpdate.school = school;
    if (church !== undefined) memberUpdate.church = church;
    if (profilePhoto) memberUpdate.profilePhoto = profilePhoto;
    if (dateOfBirth) memberUpdate.dateOfBirth = new Date(dateOfBirth);
    if (gender) memberUpdate.gender = gender;
    if (cbsLocation !== undefined) memberUpdate.cbsLocation = cbsLocation;
    if (
      group &&
      [
        "General",
        "Success",
        "Empowerment",
        "Zoe",
        "Favour",
        "Dominion",
      ].includes(group)
    )
      memberUpdate.group = group;
    memberUpdate.updatedAt = new Date();

    await Member.findOneAndUpdate(
      { phoneNumber: user.phoneNumber },
      memberUpdate,
      { new: true },
    );

    await logActivity(`updated their profile`, req.user);
    res.json({ ...user.toObject(), password: undefined });
  } catch (error) {
    console.error("Update profile error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.put("/api/profile/password", authMiddleware, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;
    const user = await User.findById(req.user._id);
    const isValid = await bcrypt.compare(currentPassword, user.password);
    if (!isValid)
      return res.status(401).json({ error: "Current password is incorrect" });
    user.password = await bcrypt.hash(newPassword, 10);
    await user.save();
    await logActivity(`changed their password`, req.user);
    res.json({ message: "Password updated successfully" });
  } catch (error) {
    console.error("Change password error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// ========== MEDIA ROUTES ==========

const mediaStorage = multer.diskStorage({
  destination: function (req, file, cb) {
    const uploadDir = path.join(__dirname, "../frontend/uploads/media");
    if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    cb(null, "media-" + uniqueSuffix + path.extname(file.originalname));
  },
});
const mediaUpload = multer({
  storage: mediaStorage,
  limits: { fileSize: 100 * 1024 * 1024 },
});

app.use(
  "/uploads/media",
  express.static(path.join(__dirname, "../frontend/uploads/media")),
);

app.get("/api/media", authMiddleware, async (req, res) => {
  try {
    const media = await Media.find().sort({ createdAt: -1 });
    res.json(media);
  } catch (error) {
    console.error("Get media error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.post(
  "/api/media",
  authMiddleware,
  mediaUpload.single("file"),
  async (req, res) => {
    try {
      const { title, type, description } = req.body;
      if (!title || !type)
        return res.status(400).json({ error: "Title and type required" });

      let fileInfo = {};
      if (req.file) {
        fileInfo = {
          fileName: req.file.originalname,
          filePath: `/uploads/media/${req.file.filename}`,
          fileSize: req.file.size,
          mimeType: req.file.mimetype,
        };
      }

      const media = new Media({
        title,
        type,
        description,
        ...fileInfo,
        uploadedBy: req.user._id,
        uploadedByName: req.user.fullName,
      });
      await media.save();
      await logActivity(`uploaded ${type} media: "${title}"`, req.user);
      res.status(201).json(media);
    } catch (error) {
      console.error("Upload media error:", error);
      res.status(500).json({ error: "Server error" });
    }
  },
);

app.delete("/api/media/:id", authMiddleware, async (req, res) => {
  try {
    const media = await Media.findById(req.params.id);
    if (!media) return res.status(404).json({ error: "Media not found" });
    if (media.filePath) {
      const fullPath = path.join(__dirname, "../frontend", media.filePath);
      if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
    }
    await Media.findByIdAndDelete(req.params.id);
    await logActivity(`deleted media: "${media.title}"`, req.user);
    res.json({ message: "Media deleted" });
  } catch (error) {
    console.error("Delete media error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// ========== ACTIVITY LOGS ==========

app.get("/api/activity-logs", authMiddleware, async (req, res) => {
  try {
    const { limit = 100 } = req.query;
    const logs = await ActivityLog.find()
      .sort({ createdAt: -1 })
      .limit(parseInt(limit));
    res.json(logs);
  } catch (error) {
    console.error("Get activity logs error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

app.delete("/api/activity-logs", authMiddleware, async (req, res) => {
  try {
    await ActivityLog.deleteMany({});
    res.json({ message: "All activity logs cleared" });
  } catch (error) {
    console.error("Clear activity logs error:", error);
    res.status(500).json({ error: "Server error" });
  }
});

// ========== DATABASE INITIALIZATION ==========

async function initializeDatabase() {
  try {
    console.log("📦 Setting up database structure...");

    // Create default groups
    const groups = [
      "General",
      "Success",
      "Empowerment",
      "Zoe",
      "Favour",
      "Dominion",
    ];
    for (const groupName of groups) {
      const existing = await Group.findOne({ name: groupName });
      if (!existing) {
        await Group.create({
          name: groupName,
          isActive: true,
          memberCount: 0,
          stewardCount: 0,
          description: `${groupName} Ministry Group`,
        });
        console.log(`   ✓ Created group: ${groupName}`);
      }
    }

    // Create default CBS locations
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
      const existing = await CBSLocation.findOne({ name: location });
      if (!existing) {
        await CBSLocation.create({
          name: location,
          status: "Active",
          memberCount: 0,
          associatedGroups: groups,
        });
        console.log(`   ✓ Created CBS location: ${location}`);
      }
    }

    console.log("✅ Database structure setup complete!");
    console.log("\n📊 Database is ready with:");
    console.log(`   - ${groups.length} groups`);
    console.log(`   - ${locations.length} CBS locations`);
    console.log("   - No users - system is clean");
    console.log("\n💡 First user to register will become Head Shepherd\n");
  } catch (error) {
    console.error("❌ Database initialization error:", error);
  }
}

// ========== START SERVER ==========

const PORT = process.env.PORT || 5000;

// Only listen if not on Vercel/Render serverless
if (process.env.NODE_ENV !== "production") {
  app.listen(PORT, () => {
    console.log(`\n🚀 MOR System Backend Server`);
    console.log(`🔗 API Base URL: https://mor-system-backend.onrender.com/api`);
    console.log(
      `💚 Health check: https://mor-system-backend.onrender.com/api/health`,
    );
    console.log(`\n✨ Ready to accept connections!\n`);
  });
}
