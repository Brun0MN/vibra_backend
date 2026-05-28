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
const queryApi = influx.getQueryApi(process.env.INFLUX_ORG);
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

  client.subscribe("vibracao/+/resultado_resumo", (err) => {
    if (err) {
      console.error("Erro ao assinar resultado_resumo:", err);
    } else {
      console.log("Assinado em vibracao/+/resultado_resumo");
    }
  });
  
  client.subscribe("vibracao/+/resultado_fft", (err) => {
    if (err) {
      console.error("Erro ao assinar resultado_fft:", err);
    } else {
      console.log("Assinado em vibracao/+/resultado_fft");
    }
  });

  client.subscribe("vibracao/+/resultado", (err) => {
    if (err) {
      console.error("Erro ao assinar vibracao/+/resultado:", err);
    } else {
      console.log("Assinado em vibracao/+/resultado");
    }
  });

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

  if (topic.includes("/resultado_resumo")) {
    try {
      const data = JSON.parse(messageBuffer.toString());
      console.log("RESULTADO_RESUMO RECEBIDO");
      console.log(data);
      await salvarResumoInflux(data);
      console.log(`Resumo Influx salvo: ${data.measurementId}`);
    } catch (error) {
      console.error("Erro resultado_resumo:", error);
    }
    return;
  }
  
  if (topic.includes("/resultado_fft")) {
    try {
      const data = JSON.parse(messageBuffer.toString());
      await salvarFFTInflux(data);
      console.log(
        `FFT Influx salva: ${data.chunkIndex + 1}/${data.totalChunks} - ${data.measurementId}`
      );
    } catch (error) {
      console.error("Erro resultado_fft:", error);
    }
    return;
  }

  if (topic.includes("/resultado")) {
    try {
      const data = JSON.parse(messageBuffer.toString());
  
      await salvarResultadoInflux(data);
  
      console.log(`Resultado salvo no Influx: ${data.measurementId}`);
    } catch (error) {
      console.error("Erro ao processar resultado MQTT:", error);
    }
  
    return;
  }

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

// function getAlarmFromVrms(vrms) { // para pontos antigos
//   const value = safeNumber(vrms);

//   if (value > 6.6) {
//     return {
//       alarmLevel: "critico",
//       alarmMessage: "Risco de dano à máquina",
//     };
//   }

//   if (value > 4.0) {
//     return {
//       alarmLevel: "alerta",
//       alarmMessage: "Vibração em nível insatisfatório",
//     };
//   }

//   return {
//     alarmLevel: "normal",
//     alarmMessage: "Operação normal",
//   };
// }

async function salvarResumoInflux(data) {
  let alarmLevel = "normal";
  let alarmMessage = "Operação normal";

  if (data.isoZone === "Zona C") {
    alarmLevel = "alerta";
    alarmMessage = "Vibração em nível insatisfatório";
  }

  if (data.isoZone === "Zona D") {
    alarmLevel = "critico";
    alarmMessage = "Risco de dano à máquina";
  }
  const p = new Point("vibracao_resumo")
    .tag("machineId", data.machineId || "desconhecido")
    .tag("sensorId", data.sensorId || "desconhecido")
    .tag("measurementId", data.measurementId || "sem_id")
    .tag("isoCategory", data.isoCategory || "catILe200")
    .floatField("vrmsVelGlobal", safeNumber(data.vrmsVelGlobal))
    .floatField("vrmsVelX", safeNumber(data.vrmsVelX))
    .floatField("vrmsVelY", safeNumber(data.vrmsVelY))
    .floatField("vrmsVelZ", safeNumber(data.vrmsVelZ))
    .floatField("vrmsVelIso", safeNumber(data.vrmsVelIso ?? data.vrmsVelGlobal))
    .floatField("vrmsVelResultante", safeNumber(data.vrmsVelResultante))
    .floatField("vrmsGlobal", safeNumber(data.vrmsGlobal))
    .floatField("dominantFreqX", safeNumber(data.dominantFreqX))
    .floatField("dominantFreqY", safeNumber(data.dominantFreqY))
    .floatField("dominantFreqZ", safeNumber(data.dominantFreqZ))
    .floatField("dominantFreqRes", safeNumber(data.dominantFreqRes))
    .floatField("measurementDurationSec", safeNumber(data.measurementDurationSec))
    .stringField("isoZone", data.isoZone || "-")
    .stringField("isoStatus", data.isoStatus || "-")
    .stringField("alarmLevel", alarmLevel)
    .stringField("alarmMessage", alarmMessage);

  writeApi.writePoint(p);
  await salvarReferenciaInfluxSeNaoExistir(data);

  const timeRms = sanitizeArray(data.timeRmsRes || data.timeRms || []);
  const duracao = safeNumber(data.measurementDurationSec || 0);
  
  for (let i = 0; i < timeRms.length; i++) {
    const t = timeRms.length > 1 && duracao > 0
      ? (i * duracao) / (timeRms.length - 1)
      : i;
  
    const pTempo = new Point("vibracao_tempo")
      .tag("machineId", data.machineId || "desconhecido")
      .tag("sensorId", data.sensorId || "desconhecido")
      .tag("measurementId", data.measurementId || "sem_id")
      .floatField("t", safeNumber(t))
      .floatField("rms", safeNumber(timeRms[i]));
  
    writeApi.writePoint(pTempo);
  }

  const timeAccRms = sanitizeArray(data.timeAccRms || []);
  const duracaoAcc = safeNumber(data.measurementDurationSec || 0);

  for (let i = 0; i < timeAccRms.length; i++) {
    const t = timeAccRms.length > 1 && duracaoAcc > 0
      ? (i * duracaoAcc) / (timeAccRms.length - 1)
      : i;

    const pTempoAcc = new Point("vibracao_tempo_acel")
      .tag("machineId", data.machineId || "desconhecido")
      .tag("sensorId", data.sensorId || "desconhecido")
      .tag("measurementId", data.measurementId || "sem_id")
      .floatField("t", safeNumber(t))
      .floatField("rms", safeNumber(timeAccRms[i]));

    writeApi.writePoint(pTempoAcc);
  }
}

// async function salvarResumoInflux(data) {
//   const p = new Point("vibracao_resumo")
//     .tag("machineId", data.machineId || "desconhecido")
//     .tag("sensorId", data.sensorId || "desconhecido")
//     .tag("measurementId", data.measurementId || "sem_id")
//     .floatField("vrmsVelX", safeNumber(data.vrmsVelX))
//     .floatField("vrmsVelY", safeNumber(data.vrmsVelY))
//     .floatField("vrmsVelZ", safeNumber(data.vrmsVelZ))
//     .floatField("vrmsVelGlobal", safeNumber(data.vrmsVelGlobal))  // resultado geral
//     .floatField("dominantFreqX", safeNumber(data.dominantFreqX))
//     .floatField("dominantFreqY", safeNumber(data.dominantFreqY))
//     .floatField("dominantFreqZ", safeNumber(data.dominantFreqZ))
//     .floatField("dominantFreqRes", safeNumber(data.dominantFreqRes))
//     .floatField("measurementDurationSec", safeNumber(data.measurementDurationSec))
//     .stringField("isoZone", data.isoZone || "-")
//     .stringField("isoStatus", data.isoStatus || "-");

//   writeApi.writePoint(p);
//   await writeApi.flush();
// }

async function salvarFFTInflux(data) {
  const fftFreq = sanitizeArray(data.fftFreq);
  const fftX = sanitizeArray(data.fftX);
  const fftY = sanitizeArray(data.fftY);
  const fftZ = sanitizeArray(data.fftZ);
  const fftRes = sanitizeArray(data.fftRes);

  function writeFFT(eixo, valores) {
    for (let i = 0; i < fftFreq.length && i < valores.length; i++) {
      const p = new Point("vibracao_fft")
        .tag("machineId", data.machineId || "desconhecido")
        .tag("sensorId", data.sensorId || "desconhecido")
        .tag("measurementId", data.measurementId || "sem_id")
        .tag("eixo", eixo)
        .floatField("freq", safeNumber(fftFreq[i]))
        .floatField("amplitude", safeNumber(valores[i]));

      writeApi.writePoint(p);
    }
  }

  writeFFT("X", fftX);
  writeFFT("Y", fftY);
  writeFFT("Z", fftZ);
  writeFFT("Res", fftRes);

  await writeApi.flush();
}

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

async function salvarResultadoInflux(data) {
  console.log("Salvando resultado completo no Influx:", data.measurementId);

  // 1) RESUMO
  const resumoPoint = new Point("vibracao_resumo")
    .tag("machineId", data.machineId || "desconhecido")
    .tag("sensorId", data.sensorId || "desconhecido")
    .tag("measurementId", data.measurementId || "sem_id")
    .floatField("vrmsVelGlobal", safeNumber(data.vrmsVelGlobal))
    .floatField("vrmsGlobal", safeNumber(data.vrmsGlobal))
    .floatField("dominantFreqX", safeNumber(data.dominantFreqX))
    .floatField("dominantFreqY", safeNumber(data.dominantFreqY))
    .floatField("dominantFreqZ", safeNumber(data.dominantFreqZ))
    .floatField("dominantFreqRes", safeNumber(data.dominantFreqRes))
    .stringField("isoZone", data.isoZone || "-")
    .stringField("isoStatus", data.isoStatus || "-");

  writeApi.writePoint(resumoPoint);

  // 2) FFT
  const fftFreq = sanitizeArray(data.fftFreq);
  const fftX = sanitizeArray(data.fftX);
  const fftY = sanitizeArray(data.fftY);
  const fftZ = sanitizeArray(data.fftZ);
  const fftRes = sanitizeArray(data.fftRes);

  function salvarFFT(eixo, valores) {
    for (let i = 0; i < fftFreq.length && i < valores.length; i++) {
      const p = new Point("vibracao_fft")
        .tag("machineId", data.machineId || "desconhecido")
        .tag("sensorId", data.sensorId || "desconhecido")
        .tag("measurementId", data.measurementId || "sem_id")
        .tag("eixo", eixo)
        .floatField("freq", safeNumber(fftFreq[i]))
        .floatField("amplitude", safeNumber(valores[i]));

      writeApi.writePoint(p);
    }
  }

  salvarFFT("X", fftX);
  salvarFFT("Y", fftY);
  salvarFFT("Z", fftZ);
  salvarFFT("Res", fftRes);

  await writeApi.flush();

  console.log("Resultado completo salvo no Influx:", data.measurementId);
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
async function salvarReferenciaInfluxSeNaoExistir(data) {
  const machineId = data.machineId || "desconhecido";

  const query = `
    from(bucket: "${process.env.INFLUX_BUCKET}")
      |> range(start: -3650d)
      |> filter(fn: (r) => r._measurement == "referencia_maquina")
      |> filter(fn: (r) => r.machineId == "${machineId}")
      |> filter(fn: (r) => r._field == "vrmsVelIso")
      |> limit(n: 1)
  `;

  let existeReferencia = false;

  await new Promise((resolve, reject) => {
    queryApi.queryRows(query, {
      next() {
        existeReferencia = true;
      },
      error(error) {
        reject(error);
      },
      complete() {
        resolve();
      },
    });
  });

  if (existeReferencia) {
    return;
  }

  const vrmsVelX = safeNumber(data.vrmsVelX);
  const vrmsVelY = safeNumber(data.vrmsVelY);
  const vrmsVelZ = safeNumber(data.vrmsVelZ);
  const vrmsVelIso = safeNumber(data.vrmsVelIso ?? data.vrmsVelGlobal);

  if (vrmsVelX <= 0 && vrmsVelY <= 0 && vrmsVelZ <= 0) {
    console.log("Referência não criada: valores de eixo zerados");
    return;
  }

  const p = new Point("referencia_maquina")
    .tag("machineId", machineId)
    .tag("measurementId", data.measurementId || "sem_id")
    .floatField("vrmsVelX", vrmsVelX)
    .floatField("vrmsVelY", vrmsVelY)
    .floatField("vrmsVelZ", vrmsVelZ)
    .floatField("vrmsVelIso", vrmsVelIso);

  writeApi.writePoint(p);
  await writeApi.flush();

  console.log(`Referência Influx criada para máquina ${machineId}`);
}

async function buscarReferenciaInflux(machineId) {
  const query = `
    from(bucket: "${process.env.INFLUX_BUCKET}")
      |> range(start: -3650d)
      |> filter(fn: (r) => r._measurement == "referencia_maquina")
      |> filter(fn: (r) => r.machineId == "${machineId}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
      |> limit(n: 1)
  `;

  let referencia = null;

  await new Promise((resolve, reject) => {
    queryApi.queryRows(query, {
      next(row, tableMeta) {
        const o = tableMeta.toObject(row);

        referencia = {
          time: o._time,
          vrmsVelX: safeNumber(o.vrmsVelX),
          vrmsVelY: safeNumber(o.vrmsVelY),
          vrmsVelZ: safeNumber(o.vrmsVelZ),
          vrmsVelIso: safeNumber(o.vrmsVelIso),
        };
      },
      error(error) {
        reject(error);
      },
      complete() {
        resolve();
      },
    });
  });

  return referencia;
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
app.get("/tendencia_influx", async (req, res) => {
  try {
    const machineId = req.query.machineId || "motor_01";

    const query = `
    from(bucket: "${process.env.INFLUX_BUCKET}")
      |> range(start: -60d)
      |> filter(fn: (r) => r._measurement == "vibracao_resumo")
      |> filter(fn: (r) => r.machineId == "${machineId}")
      |> filter(fn: (r) => contains(value: r._field, set: [
        "vrmsVelGlobal",
        "vrmsVelX",
        "vrmsVelY",
        "vrmsVelZ",
        "vrmsVelIso",
        "vrmsVelResultante",
        "alarmLevel",
        "alarmMessage"
      ]))
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
      |> tail(n: 30)
  `;

    const pontos = [];

    await new Promise((resolve, reject) => {
      queryApi.queryRows(query, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row);

          // const alarm = getAlarmFromVrms(o._value);

          // pontos.push({
          //   time: o._time,
          //   value: o._value,
          //   machineId: o.machineId,

          //   // alarmLevel: alarm.alarmLevel,
          //   // alarmMessage: alarm.alarmMessage,
          //   alarmLevel: o.alarmLevel ?? "normal",
          //   alarmMessage: o.alarmMessage ?? "",
          // });

          pontos.push({
            time: o._time,
            machineId: o.machineId,
            isoCategory: o.isoCategory ?? "catILe200",
          
            // Mantém "value" para não quebrar o app atual
            value: safeNumber(o.vrmsVelGlobal ?? o.vrmsVelIso ?? 0),
          
            // Novos campos por eixo
            vrmsVelGlobal: safeNumber(o.vrmsVelGlobal ?? o.vrmsVelIso ?? 0),
            vrmsVelX: safeNumber(o.vrmsVelX),
            vrmsVelY: safeNumber(o.vrmsVelY),
            vrmsVelZ: safeNumber(o.vrmsVelZ),
            vrmsVelIso: safeNumber(o.vrmsVelIso ?? o.vrmsVelGlobal),
            vrmsVelResultante: safeNumber(o.vrmsVelResultante),
          
            alarmLevel: o.alarmLevel ?? "normal",
            alarmMessage: o.alarmMessage ?? "",
          });


        },
        error(error) {
          reject(error);
        },
        complete() {
          resolve();
        },
      });
    });
    pontos.sort((a, b) => {
      return new Date(a.time).getTime() - new Date(b.time).getTime();
    });
    let referencia = await buscarReferenciaInflux(machineId);

    if (!referencia) {
      referencia = pontos.find((p) =>
        p.vrmsVelX > 0 || p.vrmsVelY > 0 || p.vrmsVelZ > 0
      );
    }

function variacaoPercentual(atual, ref) {
  if (!ref || ref <= 0) return 0;
  return ((atual - ref) / ref) * 100;
}

const pontosComTendencia = pontos.map((p) => {
  if (!referencia) {
    return {
      ...p,
      tendenciaAlerta: false,
      tendenciaEixo: "-",
      tendenciaVariacaoPercentual: 0,
      tendenciaMensagem: "",
    };
  }

  const variacaoX = variacaoPercentual(p.vrmsVelX, referencia.vrmsVelX);
  const variacaoY = variacaoPercentual(p.vrmsVelY, referencia.vrmsVelY);
  const variacaoZ = variacaoPercentual(p.vrmsVelZ, referencia.vrmsVelZ);

  let maiorVariacao = variacaoX;
  let eixo = "X";

  if (variacaoY > maiorVariacao) {
    maiorVariacao = variacaoY;
    eixo = "Y";
  }

  if (variacaoZ > maiorVariacao) {
    maiorVariacao = variacaoZ;
    eixo = "Z";
  }

  const tendenciaAlerta = maiorVariacao >= 25.0;

  return {
    ...p,
    variacaoX,
    variacaoY,
    variacaoZ,
    tendenciaAlerta,
    tendenciaEixo: tendenciaAlerta ? eixo : "-",
    tendenciaVariacaoPercentual: maiorVariacao,
    tendenciaMensagem: tendenciaAlerta
      ? `Aumento de ${maiorVariacao.toFixed(1)}% no eixo ${eixo} em relação à referência`
      : "",
  };
});

  res.json({
    ok: true,
    referencia: referencia
      ? {
          time: referencia.time,
          vrmsVelX: referencia.vrmsVelX,
          vrmsVelY: referencia.vrmsVelY,
          vrmsVelZ: referencia.vrmsVelZ,
          vrmsVelIso: referencia.vrmsVelIso,
        }
      : null,
    pontos: pontosComTendencia,
  });
  } catch (error) {
    console.error("Erro GET /tendencia_influx:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/fft_influx", async (req, res) => {
  try {
    const measurementId = req.query.measurementId;
    const eixo = req.query.eixo || "X";

    if (!measurementId) {
      return res.status(400).json({
        ok: false,
        error: "measurementId obrigatório",
      });
    }

    const query = `
      from(bucket: "${process.env.INFLUX_BUCKET}")
        |> range(start: -30d)
        |> filter(fn: (r) => r._measurement == "vibracao_fft")
        |> filter(fn: (r) => r.measurementId == "${measurementId}")
        |> filter(fn: (r) => r.eixo == "${eixo}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> keep(columns: ["freq", "amplitude"])
        |> sort(columns: ["freq"])
    `;

    const pontos = [];

    await new Promise((resolve, reject) => {
      queryApi.queryRows(query, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row);
          pontos.push({
            freq: Number(o.freq),
            amplitude: Number(o.amplitude),
          });
        },
        error(error) {
          reject(error);
        },
        complete() {
          resolve();
        },
      });
    });

    res.json({
      ok: true,
      measurementId,
      eixo,
      freq: pontos.map((p) => p.freq),
      amp: pontos.map((p) => p.amplitude),
    });
  } catch (error) {
    console.error("Erro GET /fft_influx:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/tempo_influx", async (req, res) => {
  try {
    const measurementId = req.query.measurementId;

    if (!measurementId) {
      return res.status(400).json({ ok: false, error: "measurementId obrigatório" });
    }

    const query = `
      from(bucket: "${process.env.INFLUX_BUCKET}")
        |> range(start: -30d)
        |> filter(fn: (r) => r._measurement == "vibracao_tempo")
        |> filter(fn: (r) => r.measurementId == "${measurementId}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["t"])
    `;

    const pontos = [];

    await new Promise((resolve, reject) => {
      queryApi.queryRows(query, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row);

          pontos.push({
            t: o.t,
            rms: o.rms,
          });
        },
        error(error) {
          reject(error);
        },
        complete() {
          resolve();
        },
      });
    });

    res.json({ ok: true, pontos });
  } catch (error) {
    console.error("Erro /tempo_influx:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/medicoes_influx", async (req, res) => {
  try {
    // const machineId = req.query.machineId || "motor_01";
    const machineId = req.query.machineId;
    let query = `
      from(bucket: "${process.env.INFLUX_BUCKET}")
        |> range(start: -30d)
        |> filter(fn: (r) => r._measurement == "vibracao_resumo")
    `;

    if (machineId && machineId.trim() !== "") {
      query += `
        |> filter(fn: (r) => r.machineId == "${machineId}")
      `;
    }

    query += `
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
    `;
    // const query = `
    //   from(bucket: "${process.env.INFLUX_BUCKET}")
    //     |> range(start: -30d)
    //     |> filter(fn: (r) => r._measurement == "vibracao_resumo")
    //     |> filter(fn: (r) => r.machineId == "${machineId}")
    //     |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    //     |> sort(columns: ["_time"], desc: true)
    // `;

    const medicoes = [];

    await new Promise((resolve, reject) => {
      queryApi.queryRows(query, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row);

          medicoes.push({
            measurementId: o.measurementId,
            machineId: o.machineId,
            sensorId: o.sensorId,
            alarmLevel: o.alarmLevel ?? "normal",
            alarmMessage: o.alarmMessage ?? "",
            createdAt: o._time,

            vrmsVelGlobal: o.vrmsVelGlobal ?? 0,
            vrmsVelX: o.vrmsVelX ?? 0,
            vrmsVelY: o.vrmsVelY ?? 0,
            vrmsVelZ: o.vrmsVelZ ?? 0,
            vrmsVelIso: o.vrmsVelIso ?? o.vrmsVelGlobal ?? 0,
            vrmsVelResultante: o.vrmsVelResultante ?? 0,
            
            vrmsGlobal: o.vrmsGlobal ?? 0,

            dominantFreqX: o.dominantFreqX ?? 0,
            dominantFreqY: o.dominantFreqY ?? 0,
            dominantFreqZ: o.dominantFreqZ ?? 0,
            dominantFreqRes: o.dominantFreqRes ?? 0,
            measurementDurationSec: o.measurementDurationSec ?? 0,

            isoZone: o.isoZone ?? "-",
            isoStatus: o.isoStatus ?? "-",
            isoCategory: o.isoCategory ?? "catILe200",
          });
        },
        error(error) {
          reject(error);
        },
        complete() {
          resolve();
        },
      });
    });

    res.json({ ok: true, medicoes });
  } catch (error) {
    console.error("Erro GET /medicoes_influx:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/tempo_acel_influx", async (req, res) => {
  try {
    const measurementId = req.query.measurementId;

    if (!measurementId) {
      return res.status(400).json({
        ok: false,
        error: "measurementId obrigatório",
      });
    }

    const query = `
      from(bucket: "${process.env.INFLUX_BUCKET}")
        |> range(start: -30d)
        |> filter(fn: (r) => r._measurement == "vibracao_tempo_acel")
        |> filter(fn: (r) => r.measurementId == "${measurementId}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["t"])
    `;

    const pontos = [];

    await new Promise((resolve, reject) => {
      queryApi.queryRows(query, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row);

          pontos.push({
            t: o.t,
            rms: o.rms,
          });
        },
        error(error) {
          reject(error);
        },
        complete() {
          resolve();
        },
      });
    });

    res.json({ ok: true, pontos });
  } catch (error) {
    console.error("Erro /tempo_acel_influx:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});
app.get("/tendencia_acel_influx", async (req, res) => {
  try {
    const machineId = req.query.machineId || "motor_01";

    const query = `
      from(bucket: "${process.env.INFLUX_BUCKET}")
        |> range(start: -60d)
        |> filter(fn: (r) => r._measurement == "vibracao_resumo")
        |> filter(fn: (r) => r.machineId == "${machineId}")
        |> filter(fn: (r) => r._field == "vrmsGlobal")
        |> group()
        |> sort(columns: ["_time"])
        |> tail(n: 30)
        |> sort(columns: ["_time"])
    `;

    const pontos = [];

    await new Promise((resolve, reject) => {
      queryApi.queryRows(query, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row);

          pontos.push({
            time: o._time,
            value: o._value,
            machineId: o.machineId,
            alarmLevel: o.alarmLevel ?? "normal",
            alarmMessage: o.alarmMessage ?? "",
          });
        },
        error(error) {
          reject(error);
        },
        complete() {
          resolve();
        },
      });
    });

    res.json({ ok: true, pontos });
  } catch (error) {
    console.error("Erro GET /tendencia_acel_influx:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});
app.post("/iniciar_medicao", (req, res) => {
  const machineId = req.body.machineId || "motor_01";
  const duracao = req.body.duracao || 5;

  // const topic = `vibracao/${machineId}/comando`;
  const topic = `vibracao/comando`;

  const payload = JSON.stringify({
    acao: "iniciar_medicao",
    duracao,
    machineId,
    isoCategory: req.body.isoCategory || "catILe200",
  });

  client.publish(topic, payload, (err) => {
    if (err) {
      return res.status(500).json({ ok: false, error: err.message });
    }

    res.json({ ok: true, topic, payload });
  });
});
app.get("/machines", async (req, res) => {
  try {
    const query = `
    from(bucket: "${process.env.INFLUX_BUCKET}")
      |> range(start: -365d)
      |> filter(fn: (r) => r._measurement == "vibracao_resumo")
      |> filter(fn: (r) => r._field == "vrmsVelGlobal")
      |> sort(columns: ["_time"], desc: true)
  `;

    const maquinasMap = new Map();

    await new Promise((resolve, reject) => {
      queryApi.queryRows(query, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row);
    
          if (!maquinasMap.has(o.machineId)) {
            maquinasMap.set(o.machineId, {
              machineId: o.machineId,
              isoCategory: o.isoCategory ?? "catILe200",
            });
          }
        },
        error(error) {
          reject(error);
        },
        complete() {
          resolve();
        },
      });
    });
    
    res.json({
      ok: true,
      maquinas: Array.from(maquinasMap.values()),
    });

  } catch (error) {
    console.error(error);

    res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});
app.listen(PORT, () => {
  console.log(`HTTP server rodando na porta ${PORT}`);
});