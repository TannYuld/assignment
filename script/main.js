// node_modules/@tannyuld/ridbf/dist/parser.js
function parseDate(date) {
  const parts = date.split("-");
  if (parts.length !== 3) {
    throw new Error("Invalid date format. Expected YYYY-MM-DD variation.");
  }
  const year = parts[0];
  const month = parts[1].padStart(2, "0");
  const day = parts[2].padStart(2, "0");
  const validIsoStr = `${year}-${month}-${day}`;
  return new Date(validIsoStr);
}
function splitContent(content) {
  const contentLinesRaw = content.split(`
`).filter((line) => {
    return !line.startsWith("#");
  }).map((line) => {
    if (line.startsWith("\\#")) {
      return line.substring(1);
    }
    return line;
  });
  let contentLines = [];
  let lastValueLineIndex = 0;
  let combinedLineCount = 0;
  for (let i = 0;i < contentLinesRaw.length; i++) {
    const line = contentLinesRaw[i];
    if (line == "") {
      contentLines[i - combinedLineCount] = undefined;
      lastValueLineIndex = i - combinedLineCount;
      continue;
    }
    const numeric = !isNaN(Number(line.replaceAll("-", "")));
    const array = line.startsWith("[") && line.endsWith("]");
    const date = line.includes("-") && !array && numeric;
    let value = date ? parseDate(line) : array && numeric ? line.split(",").map(Number) : array ? line.replace(/^\[|\]$/g, "").split(",") : numeric ? Number(line) : line;
    if (line.endsWith("[\\]")) {
      contentLines[lastValueLineIndex] += line.split("[\\]")[0];
      combinedLineCount += 1;
    } else {
      lastValueLineIndex = i - combinedLineCount;
      contentLines[lastValueLineIndex] = value;
    }
  }
  return contentLines;
}
function parse(content, fields) {
  const contentLines = splitContent(content);
  const result = [];
  const len = contentLines.length - contentLines.length % fields.length;
  for (let i = 0;i < len; i += fields.length) {
    let obj = {};
    for (let j = 0;j < fields.length; j++) {
      const lineValue = contentLines[j + i];
      const currentField = fields[j];
      const value = lineValue === undefined ? undefined : lineValue;
      if (typeof currentField === "string") {
        obj[fields[j]] = value;
      } else {
        obj[Object.keys(fields[0])[0]] = value;
      }
    }
    result.push(obj);
  }
  return result;
}

// node_modules/@tannyuld/ridbf/dist/main.js
async function loadFile(filePath, cacheType) {
  try {
    if (filePath.startsWith("../"))
      throw new Error("Cannot fetch parent folder of server");
    const response = await fetch(!filePath.startsWith("./") ? "./" + filePath : filePath, { cache: cacheType ? cacheType : "default" });
    if (!response.ok)
      throw new Error("File not found");
    return response.text();
  } catch (error) {
    return Promise.reject(error);
  }
}
var CacheType;
(function(CacheType2) {
  CacheType2["Default"] = "default";
  CacheType2["ForceCache"] = "force-cache";
  CacheType2["NoCache"] = "no-cache";
  CacheType2["NoStore"] = "no-store";
  CacheType2["OnlyIfCached"] = "only-if-cached";
  CacheType2["Reload"] = "reload";
})(CacheType || (CacheType = {}));

class RIDBHandle {
  dbname;
  path;
  schema;
  db;
  fetchOptions;
  onDataRetrivedSuccesfullyEvents;
  onDataRetriveFailedEvents;
  static knownStores = new Map;
  constructor(dbname, schema, fetchOptions) {
    this.dbname = dbname;
    this.schema = schema;
    this.path = fetchOptions?.customPath === undefined ? this.dbname : fetchOptions.customPath;
    this.onDataRetrivedSuccesfullyEvents = [];
    this.onDataRetriveFailedEvents = [];
    this.fetchOptions = fetchOptions ? fetchOptions : null;
    this.db = null;
  }
  static open(dbname, schema, fetchOptions) {
    return new RIDBHandle(dbname, schema, fetchOptions);
  }
  onDataRetrivedSuccesfully(event) {
    this.onDataRetrivedSuccesfullyEvents.push(event);
  }
  onDataRetriveFailed(event) {
    this.onDataRetriveFailedEvents.push(event);
  }
  async fetch() {
    try {
      this.db = await this._retrive();
      if (!await this.isDataUpToDate()) {
        await this.updateDatabaseData();
        await this.updateDataIntegrity();
      }
      this.onDataRetrivedSuccesfullyEvents.forEach((event) => {
        event();
      });
    } catch (error) {
      this.onDataRetriveFailedEvents.forEach((event) => {
        event();
      });
    }
  }
  async findAll() {
    return new Promise((resolve, reject) => {
      const store = this.getObjectStore();
      const request = store.getAll();
      request.onsuccess = () => {
        resolve(request.result);
      };
      request.onerror = () => {
        reject(request.error);
      };
    });
  }
  async findById(id) {
    return new Promise((resolve, reject) => {
      const store = this.getObjectStore();
      const request = store.get(id);
      request.onsuccess = () => {
        resolve(request.result);
      };
      request.onerror = () => {
        reject(request.error);
      };
    });
  }
  async findByIndex(indexKey, indexValue) {
    return new Promise((resolve, reject) => {
      const store = this.getObjectStore();
      const index = store.index(String(indexKey));
      const request = index.getAll(indexValue);
      request.onsuccess = () => {
        resolve(request.result);
      };
      request.onerror = () => {
        reject(request.error);
      };
    });
  }
  async findFirstByIndex(indexKey, indexValue) {
    return new Promise((resolve, reject) => {
      const store = this.getObjectStore();
      const index = store.index(String(indexKey));
      const request = index.get(indexValue);
      request.onsuccess = () => {
        resolve(request.result);
      };
      request.onerror = () => {
        reject(request.error);
      };
    });
  }
  async _retrive() {
    const stores = RIDBHandle.knownStores.get(this.dbname) ?? new Set;
    stores.add(this.getDataPath());
    RIDBHandle.knownStores.set(this.dbname, stores);
    const probe = await this.probeDatabase();
    return new Promise((resolve, reject) => {
      const needsUpgrade = !probe.storeNames.includes(this.getDataPath());
      const nextVersion = needsUpgrade ? probe.version + 1 : probe.version;
      const request = indexedDB.open(this.dbname, nextVersion || undefined);
      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        this.constructDatabaseStructure(db);
      };
      request.onsuccess = (event) => {
        resolve(event.target.result);
      };
      request.onerror = (_) => {
        reject("Error");
      };
      request.onblocked = (_) => {
        reject("Blocked: another tab is holding an older version open");
      };
    });
  }
  async probeDatabase() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbname);
      request.onsuccess = (event) => {
        const db = event.target.result;
        const result = {
          version: db.version,
          storeNames: Array.from(db.objectStoreNames)
        };
        db.close();
        resolve(result);
      };
      request.onerror = () => reject(request.error);
      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        resolve({ version: 1, storeNames: [] });
      };
    });
  }
  constructDatabaseStructure(db) {
    if (!db.objectStoreNames.contains(this.getDataPath())) {
      const store = db.createObjectStore(this.getDataPath(), { keyPath: "id", autoIncrement: true });
      this.schema.forEach((field) => {
        if (typeof field === "object") {
          const fieldName = Object.keys(field)[0];
          const fieldParameters = field[fieldName];
          store.createIndex(fieldName, fieldName, fieldParameters);
        }
      });
    }
  }
  async updateDatabaseData() {
    const remoteFileContent = await loadFile(this.getDataPath(), this.fetchOptions?.dataCache);
    const resultSet = parse(remoteFileContent, this.schema);
    if (this.db === null) {
      return;
    }
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(this.getDataPath(), "readwrite");
      const store = transaction.objectStore(this.getDataPath());
      store.clear();
      resultSet.forEach((result) => {
        store.add(result);
      });
      transaction.oncomplete = () => resolve();
      transaction.onerror = () => reject(transaction.error);
    });
  }
  async updateDataIntegrity() {
    const remoteFileIntegrity = await loadFile(this.getIntegrtiyPath(), this.fetchOptions?.integrityCache);
    localStorage.setItem(this.getIntegrtiyPath(), remoteFileIntegrity);
  }
  async isDataUpToDate() {
    const localChecksum = localStorage.getItem(this.getIntegrtiyPath());
    const remoteChecksum = await loadFile(this.getIntegrtiyPath(), this.fetchOptions?.integrityCache);
    return localChecksum === remoteChecksum;
  }
  getObjectStore() {
    if (this.db === null) {
      throw "Null database exception";
    }
    const transaction = this.db.transaction(this.getDataPath(), "readonly");
    return transaction.objectStore(this.getDataPath());
  }
  getDataPath() {
    return this.path + ".ridb";
  }
  getIntegrtiyPath() {
    return this.path + ".ridbi";
  }
}

// src/script/main.ts
var BLOGPOST_KEY = "blogposts";
var BlogPostSchema = [
  { title: { unique: false } },
  "date",
  "tags",
  "content"
];
function retrieveData() {
  const retrievedData = localStorage.getItem(BLOGPOST_KEY);
  if (retrievedData === null) {
    return [];
  }
  const result = JSON.parse(retrievedData);
  result.map((post) => {
    if (typeof post.date !== typeof Date) {
      post.date = new Date(post.date);
    }
    return post;
  });
  return result;
}
async function fetchDataIfIntegrityNotMatch() {
  const handle = RIDBHandle.open("blogpost", BlogPostSchema, { dataCache: CacheType.NoCache, integrityCache: CacheType.NoCache });
  await handle.fetch();
  const result = await handle.findAll();
  if (result !== undefined || result !== null) {
    localStorage.setItem(BLOGPOST_KEY, JSON.stringify(result));
  }
  return result;
}
export {
  retrieveData,
  fetchDataIfIntegrityNotMatch,
  BlogPostSchema
};
