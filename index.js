console.log("Backend iniciando...");

const { InfluxDB, Point } = require('@influxdata/influxdb-client');

const influx = new InfluxDB({
  url: process.env.INFLUX_URL,
  token: process.env.INFLUX_TOKEN,
});

const writeApi = influx.getWriteApi(
  process.env.INFLUX_ORG,
  process.env.INFLUX_BUCKET
);
const express = require("express");
const mqtt = require("mqtt");

//const serviceAccount = require("./serviceAccountKey.json");
const admin = require("firebase-admin");

const serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const app = express();
app.use(express.json({ limit: "10mb" }));

//const PORT = 3000;
const PORT = process.env.PORT || 3000;
const db = admin.firestore();

// ===== MQTT CONFIG =====
const MQTT_BROKER_URL = "mqtts://geccf906.ala.us-east-1.emqxsl.com:8883";
const MQTT_USERNAME = "admin";
const MQTT_PASSWORD = "asdm591g8";

console.log("Tentando conectar no broker MQTT...");

const client = mqtt.connect(MQTT_BROKER_URL, {
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
  rejectUnauthorized: false,
});

client.on("error", (err) => {
  console.error("Erro MQTT:", err);
});

client.on("offline", () => {
  console.log("MQTT offline");
});

client.on("reconnect", () => {
  console.log("Reconectando no MQTT...");
});

client.on("connect", () => {
  console.log("Conectado ao broker MQTT");

  client.subscribe("vibracao/+/resumo", (err) => {
    if (err) {
      console.error("Erro ao assinar tópico resumo:", err);
    } else {
      console.log("Assinado em vibracao/+/resumo");
    }
  });

  client.subscribe("vibracao/+/chunk", (err) => {
    if (err) {
      console.error("Erro ao assinar vibracao/+/chunk:", err);
    } else {
      console.log("Assinado em vibracao/+/chunk");
    }
  });

  client.subscribe("vibracao/+/medicao", (err) => {
    if (err) {
      console.error("Erro ao assinar tópico medicao:", err);
    } else {
      console.log("Assinado em vibracao/+/medicao");
    }
  });
});

client.on("message", async (topic, messageBuffer) => {

  if (topic.includes("/chunk")) {
    try {
      const data = JSON.parse(messageBuffer.toString());
  
      await salvarChunk(data);
  
      console.log(
        `Chunk MQTT recebido: ${data.chunkIndex + 1}/${data.totalChunks} - ${data.measurementId}`
      );
    } catch (error) {
      console.error("Erro ao processar chunk MQTT:", error);
    }
  
    return;
  }
  // try {
  //   const payload = JSON.parse(messageBuffer.toString());
  //   console.log(`Mensagem recebida em ${topic}`);

  //   if (topic.endsWith("/resumo")) {
  //     await handleResumo(payload);
  //   } else if (topic.endsWith("/medicao")) {
  //     await handleMedicao(payload);
  //   }
  // } catch (error) {
  //   console.error("Erro ao processar mensagem MQTT:", error);
  // }
});

async function salvarChunk(payload) {
  if (!payload.measurementId) {
    throw new Error("chunk sem measurementId");
  }

  const chunkDoc = {
    measurementId: payload.measurementId,
    machineId: payload.machineId || "desconhecido",
    sensorId: payload.sensorId || "desconhecido",

    chunkIndex: payload.chunkIndex ?? 0,
    totalChunks: payload.totalChunks ?? 1,

    samplingFrequency: payload.samplingFrequency || 0,
    samplesInChunk: payload.samplesInChunk || 0,

    timeAxis: sanitizeArray(payload.timeAxis),
    timeX: sanitizeArray(payload.timeX),
    timeY: sanitizeArray(payload.timeY),
    timeZ: sanitizeArray(payload.timeZ),
    timeRes: sanitizeArray(payload.timeRes),

    fftFreq: sanitizeArray(payload.fftFreq),
    fftX: sanitizeArray(payload.fftX),
    fftY: sanitizeArray(payload.fftY),
    fftZ: sanitizeArray(payload.fftZ),
    fftRes: sanitizeArray(payload.fftRes),

    dominantFreqX: safeNumber(payload.dominantFreqX),
    dominantFreqY: safeNumber(payload.dominantFreqY),
    dominantFreqZ: safeNumber(payload.dominantFreqZ),
    dominantFreqRes: safeNumber(payload.dominantFreqRes),

    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  await db.collection("medicoes_chunks").add(chunkDoc);

  if (chunkDoc.chunkIndex === chunkDoc.totalChunks - 1) {
    const resumo = {
      machineId: chunkDoc.machineId,
      sensorId: chunkDoc.sensorId,
      measurementId: chunkDoc.measurementId,
  
      vrmsX: safeNumber(payload.vrmsX),
      vrmsY: safeNumber(payload.vrmsY),
      vrmsZ: safeNumber(payload.vrmsZ),
      vrmsGlobal: safeNumber(payload.vrmsGlobal),
      vrmsVelGlobal: safeNumber(payload.vrmsVelGlobal),
  
      dominantFreqX: safeNumber(payload.dominantFreqX),
      dominantFreqY: safeNumber(payload.dominantFreqY),
      dominantFreqZ: safeNumber(payload.dominantFreqZ),
      dominantFreqRes: safeNumber(payload.dominantFreqRes),
  
      isoZone: payload.isoZone || "-",
      isoStatus: payload.isoStatus || "-",
    };
  
    await db.collection("medicoes").add({
      ...resumo,
      samplingFrequency: chunkDoc.samplingFrequency,
      samples: chunkDoc.totalChunks * chunkDoc.samplesInChunk,
      measurementDurationSec:
        (chunkDoc.totalChunks * chunkDoc.samplesInChunk) /
        chunkDoc.samplingFrequency,
  
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      type: "medicao",
    });
  
    console.log(`Resumo da medição criado: ${chunkDoc.measurementId}`);

    salvarInfluxResumo(resumo);
  }
}

async function handleResumo(payload) {
  validateResumo(payload);

  const doc = {
    machineId: payload.machineId,
    sensorId: payload.sensorId || "desconhecido",
    measurementId: payload.measurementId || null,
    timestamp: payload.timestamp || Date.now(),
    samplingFrequency: payload.samplingFrequency || 0,
    samples: payload.samples || 0,
    vrmsX: safeNumber(payload.vrmsX),
    vrmsY: safeNumber(payload.vrmsY),
    vrmsZ: safeNumber(payload.vrmsZ),
    vrmsGlobal: safeNumber(payload.vrmsGlobal),
    dominantFreqX: safeNumber(payload.dominantFreqX),
    dominantFreqY: safeNumber(payload.dominantFreqY),
    dominantFreqZ: safeNumber(payload.dominantFreqZ),
    dominantFreqRes: safeNumber(payload.dominantFreqRes),
    isoZone: payload.isoZone || "-",
    isoStatus: payload.isoStatus || "-",
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    type: "resumo",
  };

  await db.collection("medicoes").add(doc);
  console.log("Resumo salvo no Firestore");
}

async function handleMedicao(payload) {
  validateMedicao(payload);

  const timestamp = payload.timestamp || Date.now();

  const fullMeasurement = {
    machineId: payload.machineId,
    sensorId: payload.sensorId || "desconhecido",
    measurementId: payload.measurementId || null,
    timestamp,
    samplingFrequency: payload.samplingFrequency || 0,
    samples: payload.samples || 0,
    measurementDurationSec: safeNumber(payload.measurementDurationSec),
    vrmsX: safeNumber(payload.vrmsX),
    vrmsY: safeNumber(payload.vrmsY),
    vrmsZ: safeNumber(payload.vrmsZ),
    vrmsGlobal: safeNumber(payload.vrmsGlobal),
    dominantFreqX: safeNumber(payload.dominantFreqX),
    dominantFreqY: safeNumber(payload.dominantFreqY),
    dominantFreqZ: safeNumber(payload.dominantFreqZ),
    dominantFreqRes: safeNumber(payload.dominantFreqRes),
    isoZone: payload.isoZone || "-",
    isoStatus: payload.isoStatus || "-",
    timeAxis: sanitizeArray(payload.timeAxis),
    timeX: sanitizeArray(payload.timeX),
    timeY: sanitizeArray(payload.timeY),
    timeZ: sanitizeArray(payload.timeZ),
    timeRes: sanitizeArray(payload.timeRes),
    fftFreq: sanitizeArray(payload.fftFreq),
    fftX: sanitizeArray(payload.fftX),
    fftY: sanitizeArray(payload.fftY),
    fftZ: sanitizeArray(payload.fftZ),
    fftRes: sanitizeArray(payload.fftRes),
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  await db.collection("medicoes_completas").add(fullMeasurement);

  const doc = {
    machineId: payload.machineId,
    sensorId: payload.sensorId || "desconhecido",
    measurementId: payload.measurementId || null,
    timestamp,
    samplingFrequency: payload.samplingFrequency || 0,
    samples: payload.samples || 0,
    measurementDurationSec: safeNumber(payload.measurementDurationSec),
    vrmsX: safeNumber(payload.vrmsX),
    vrmsY: safeNumber(payload.vrmsY),
    vrmsZ: safeNumber(payload.vrmsZ),
    vrmsGlobal: safeNumber(payload.vrmsGlobal),
    dominantFreqX: safeNumber(payload.dominantFreqX),
    dominantFreqY: safeNumber(payload.dominantFreqY),
    dominantFreqZ: safeNumber(payload.dominantFreqZ),
    dominantFreqRes: safeNumber(payload.dominantFreqRes),
    isoZone: payload.isoZone || "-",
    isoStatus: payload.isoStatus || "-",
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    type: "medicao",
  };

  await db.collection("medicoes").add(doc);
  console.log("Medição completa salva no Firestore");
}

function salvarInfluxResumo(data) {
  console.log("Salvando resumo no Influx:", data.measurementId);
  const point = new Point("vibracao")
    .tag("machineId", data.machineId)
    .floatField("vrmsVelGlobal", data.vrmsVelGlobal || 0)
    .floatField("vrmsGlobal", data.vrmsGlobal || 0)
    .floatField("dominantFreqX", data.dominantFreqX || 0)
    .floatField("dominantFreqY", data.dominantFreqY || 0)
    .floatField("dominantFreqZ", data.dominantFreqZ || 0)
    .stringField("isoZone", data.isoZone || "")
    .stringField("isoStatus", data.isoStatus || "");

  writeApi.writePoint(point);
  writeApi.flush().catch((err) => {
    console.error("Erro ao enviar para Influx:", err);
  });
}

function validateResumo(payload) {
  if (!payload.machineId) {
    throw new Error("Resumo sem machineId");
  }
}

function validateMedicao(payload) {
  if (!payload.machineId) {
    throw new Error("Medição sem machineId");
  }
  if (!Array.isArray(payload.timeAxis) || !Array.isArray(payload.fftFreq)) {
    throw new Error("Medição sem vetores válidos");
  }
}

function safeNumber(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return num;
}

function sanitizeArray(arr) {
  if (!Array.isArray(arr)) return [];
  return arr.map((v) => {
    const num = Number(v);
    return Number.isFinite(num) ? num : 0;
  });
}

app.post("/medicao", async (req, res) => {
  try {
    const payload = req.body;

    validateMedicao(payload);

    const timestamp = payload.timestamp || Date.now();

    const fullMeasurement = {
      machineId: payload.machineId,
      sensorId: payload.sensorId || "desconhecido",
      measurementId: payload.measurementId || null,
      timestamp,
      samplingFrequency: payload.samplingFrequency || 0,
      samples: payload.samples || 0,
      measurementDurationSec: safeNumber(payload.measurementDurationSec),
      vrmsX: safeNumber(payload.vrmsX),
      vrmsY: safeNumber(payload.vrmsY),
      vrmsZ: safeNumber(payload.vrmsZ),
      vrmsGlobal: safeNumber(payload.vrmsGlobal),
      dominantFreqX: safeNumber(payload.dominantFreqX),
      dominantFreqY: safeNumber(payload.dominantFreqY),
      dominantFreqZ: safeNumber(payload.dominantFreqZ),
      dominantFreqRes: safeNumber(payload.dominantFreqRes),
      isoZone: payload.isoZone || "-",
      isoStatus: payload.isoStatus || "-",
      timeAxis: sanitizeArray(payload.timeAxis),
      timeX: sanitizeArray(payload.timeX),
      timeY: sanitizeArray(payload.timeY),
      timeZ: sanitizeArray(payload.timeZ),
      timeRes: sanitizeArray(payload.timeRes),
      fftFreq: sanitizeArray(payload.fftFreq),
      fftX: sanitizeArray(payload.fftX),
      fftY: sanitizeArray(payload.fftY),
      fftZ: sanitizeArray(payload.fftZ),
      fftRes: sanitizeArray(payload.fftRes),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await db.collection("medicoes_completas").add(fullMeasurement);

    const doc = {
      machineId: payload.machineId,
      sensorId: payload.sensorId || "desconhecido",
      measurementId: payload.measurementId || null,
      timestamp,
      samplingFrequency: payload.samplingFrequency || 0,
      samples: payload.samples || 0,
      measurementDurationSec: safeNumber(payload.measurementDurationSec),
      vrmsX: safeNumber(payload.vrmsX),
      vrmsY: safeNumber(payload.vrmsY),
      vrmsZ: safeNumber(payload.vrmsZ),
      vrmsGlobal: safeNumber(payload.vrmsGlobal),
      dominantFreqX: safeNumber(payload.dominantFreqX),
      dominantFreqY: safeNumber(payload.dominantFreqY),
      dominantFreqZ: safeNumber(payload.dominantFreqZ),
      dominantFreqRes: safeNumber(payload.dominantFreqRes),
      isoZone: payload.isoZone || "-",
      isoStatus: payload.isoStatus || "-",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      type: "medicao",
    };

    await db.collection("medicoes").add(doc);

    console.log("Medição completa recebida via HTTP e salva no Firestore com sucesso");
    res.status(200).json({ ok: true });
  } catch (error) {
    console.error("Erro ao processar medição via HTTP:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/medicao_chunk", async (req, res) => {
  try {
    const payload = req.body;

    if (!payload.measurementId) {
      throw new Error("chunk sem measurementId");
    }

    const chunkDoc = {
      measurementId: payload.measurementId,
      machineId: payload.machineId || "desconhecido",
      sensorId: payload.sensorId || "desconhecido",
    
      chunkIndex: payload.chunkIndex ?? 0,
      totalChunks: payload.totalChunks ?? 1,
    
      samplingFrequency: payload.samplingFrequency || 0,
      samplesInChunk: payload.samplesInChunk || 0,
    
      timeAxis: sanitizeArray(payload.timeAxis),
      timeX: sanitizeArray(payload.timeX),
      timeY: sanitizeArray(payload.timeY),
      timeZ: sanitizeArray(payload.timeZ),
      timeRes: sanitizeArray(payload.timeRes),
    
      // 👉 ADICIONE ISSO
      fftFreq: sanitizeArray(payload.fftFreq),
      fftX: sanitizeArray(payload.fftX),
      fftY: sanitizeArray(payload.fftY),
      fftZ: sanitizeArray(payload.fftZ),
      fftRes: sanitizeArray(payload.fftRes),

      dominantFreqX: safeNumber(payload.dominantFreqX),
      dominantFreqY: safeNumber(payload.dominantFreqY),
      dominantFreqZ: safeNumber(payload.dominantFreqZ),
      dominantFreqRes: safeNumber(payload.dominantFreqRes),
    
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await db.collection("medicoes_chunks").add(chunkDoc);

    console.log(
      `Chunk recebido: ${chunkDoc.chunkIndex + 1}/${chunkDoc.totalChunks} - ${chunkDoc.measurementId}`
    );

    if (chunkDoc.chunkIndex === chunkDoc.totalChunks - 1) {
      console.log("Criando resumo em medicoes:", {
        measurementId: chunkDoc.measurementId,
        machineId: chunkDoc.machineId,
        chunkIndex: chunkDoc.chunkIndex,
        totalChunks: chunkDoc.totalChunks,
        vrmsVelGlobal: payload.vrmsVelGlobal,
      });
      const resumoRef = await db.collection("medicoes").add({
        machineId: chunkDoc.machineId,
        sensorId: chunkDoc.sensorId,
        measurementId: chunkDoc.measurementId,
    
        samplingFrequency: chunkDoc.samplingFrequency,
        samples: chunkDoc.totalChunks * chunkDoc.samplesInChunk,
        measurementDurationSec:
          (chunkDoc.totalChunks * chunkDoc.samplesInChunk) /
          chunkDoc.samplingFrequency,
    
        vrmsX: safeNumber(payload.vrmsX),
        vrmsY: safeNumber(payload.vrmsY),
        vrmsZ: safeNumber(payload.vrmsZ),
        vrmsGlobal: safeNumber(payload.vrmsGlobal),
        vrmsVelGlobal: safeNumber(payload.vrmsVelGlobal),
    
        dominantFreqX: safeNumber(payload.dominantFreqX),
        dominantFreqY: safeNumber(payload.dominantFreqY),
        dominantFreqZ: safeNumber(payload.dominantFreqZ),
        dominantFreqRes: safeNumber(payload.dominantFreqRes),
    
        isoZone: payload.isoZone || "-",
        isoStatus: payload.isoStatus || "-",
    
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        type: "medicao",
      });
      console.log("Resumo salvo em medicoes com ID:", resumoRef.id);
      console.log(`Resumo da medição criado: ${chunkDoc.measurementId}`);
    }

    res.status(200).json({ ok: true });
  } catch (error) {
    console.error("Erro ao processar chunk:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});
app.get("/medicoes", async (req, res) => {
  try {
    const machineId = req.query.machineId;

    let query = db.collection("medicoes")
      .where("type", "==", "medicao")
      .limit(100);

    if (machineId && machineId.trim() !== "") {
      query = query.where("machineId", "==", machineId.trim());
    }

    const snapshot = await query.get();

    const medicoes = snapshot.docs.map((doc) => ({
      id: doc.id,
      ...doc.data(),
    }));

    medicoes.sort((a, b) => {
      const ta = a.createdAt?._seconds ?? 0;
      const tb = b.createdAt?._seconds ?? 0;
      return tb - ta;
    });

    res.json({ ok: true, medicoes });
  } catch (error) {
    console.error("Erro GET /medicoes:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});
app.get("/medicoes/:measurementId/chunks", async (req, res) => {
  try {
    const measurementId = req.params.measurementId;

    const snapshot = await db.collection("medicoes_chunks")
      .where("measurementId", "==", measurementId)
      .get();

    const chunks = snapshot.docs.map((doc) => ({
      id: doc.id,
      ...doc.data(),
    }));

    chunks.sort((a, b) => {
      const ai = a.chunkIndex ?? 0;
      const bi = b.chunkIndex ?? 0;
      return ai - bi;
    });

    res.json({ ok: true, chunks });
  } catch (error) {
    console.error("Erro GET chunks:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});
app.get("/tendencia", async (req, res) => {
  try {
    const machineId = req.query.machineId;

    let query = db.collection("medicoes")
      .where("type", "==", "medicao")
      .limit(100);

    if (machineId && machineId.trim() !== "") {
      query = query.where("machineId", "==", machineId.trim());
    }

    const snapshot = await query.get();

    const pontos = snapshot.docs.map((doc) => {
      const data = doc.data();

      return {
        id: doc.id,
        machineId: data.machineId,
        measurementId: data.measurementId,
        vrmsVelGlobal: data.vrmsVelGlobal ?? data.vrmsGlobal ?? 0,
        isoZone: data.isoZone ?? "-",
        isoStatus: data.isoStatus ?? "-",
        createdAt: data.createdAt,
      };
    });

    pontos.sort((a, b) => {
      const ta = a.createdAt?._seconds ?? 0;
      const tb = b.createdAt?._seconds ?? 0;
      return ta - tb;
    });

    res.json({ ok: true, pontos });
  } catch (error) {
    console.error("Erro GET /tendencia:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});
app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "vibra_backend",
    status: "online",
  });
});
app.listen(PORT, () => {
  console.log(`HTTP server rodando na porta ${PORT}`);
});
