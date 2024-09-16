const express = require('express');
const multer = require('multer');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const mime = require('mime-types');
const { v4: uuidv4 } = require('uuid');

const app = express();
const port = process.env.PORT || 3000;

const uploadsDir = 'uploads/';
const models3DDir = path.join(uploadsDir, 'models3D');
const targetsDir = path.join(uploadsDir, 'targets');
const metadataFilePath = path.join(__dirname, 'metadata.json');

const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    let dir;
    if (['.gltf', '.glb'].includes(path.extname(file.originalname).toLowerCase())) {
      dir = models3DDir;
    } else if (path.extname(file.originalname).toLowerCase() === '.mind') {
      dir = targetsDir;
    } else {
      return cb(new Error('Extensión de archivo no permitida'), null);
    }
    cb(null, dir);
  },
  filename: function (req, file, cb) {
    const uniqueName = uuidv4();
    cb(null, `${uniqueName}${path.extname(file.originalname)}`);
  }
});

[models3DDir, targetsDir].forEach(dir => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
});

if (!fs.existsSync(metadataFilePath)) {
  fs.writeFileSync(metadataFilePath, JSON.stringify({}));
}

const loadMetadata = async () => {
  try {
    const data = await fs.promises.readFile(metadataFilePath, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    if (err.code === 'ENOENT') return {};
    console.error('Error al cargar metadatos:', err);
    return {};
  }
};

const saveMetadata = async (metadata) => {
  try {
    const data = JSON.stringify(metadata, null, 2);
    await fs.promises.writeFile(metadataFilePath, data, 'utf8');
  } catch (err) {
    console.error('Error al guardar metadatos:', err);
  }
};

const calculateFileHash = (filePath) => {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', data => hash.update(data));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
};

const upload = multer({ storage: storage });

app.post('/upload', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ message: 'No se ha subido ningún archivo' });
  }

  const uniqueName = req.file.filename;
  const originalName = req.file.originalname;
  const filePath = path.join(req.file.destination, uniqueName);
  const fileExtension = path.extname(originalName).toLowerCase();
  const metadata = await loadMetadata();

  const existingFile = Object.values(metadata).find(meta => meta.originalName === originalName);

  if (existingFile) {
    existingFile.timestamp = Date.now();
    existingFile.date = new Date().toISOString();
    await saveMetadata(metadata);

    fs.unlink(filePath, err => {
      if (err) console.error('Error al eliminar el archivo existente:', err);
    });
    return res.json({ message: 'El archivo ya existe. Metadata actualizada', fileName: existingFile.uniqueName });
  }

  let expectedDirectory;
  if (['.gltf', '.glb'].includes(fileExtension)) {
    expectedDirectory = models3DDir;
  } else if (fileExtension === '.mind') {
    expectedDirectory = targetsDir;
  } else {
    fs.unlink(filePath, err => {
      if (err) console.error('Error al eliminar el archivo con extensión incorrecta:', err);
    });
    return res.status(400).json({ message: 'Extensión de archivo no permitida' });
  }

  try {
    if (req.file.destination !== expectedDirectory) {
      fs.unlink(filePath, err => {
        if (err) {
          console.error('Error al eliminar el archivo fuera del directorio esperado:', err);
        }
      });
      return res.status(500).json({ message: 'El archivo se guardó en un directorio incorrecto' });
    }

    const fileHash = await calculateFileHash(filePath);
    const mimeType = mime.lookup(filePath) || 'application/octet-stream';

    const fileMetadata = {
      timestamp: Date.now(),
      size: req.file.size,
      mimetype: mimeType,
      date: new Date().toISOString(),
      originalName: originalName,
      hash: fileHash,
      uniqueName: uniqueName
    };

    metadata[uniqueName] = fileMetadata;
    await saveMetadata(metadata);

    res.json({ message: 'Archivo recibido y almacenado', fileName: uniqueName });
  } catch (err) {
    fs.unlink(filePath, err => {
      if (err) {
        console.error('Error al eliminar el archivo después del fallo en guardar metadatos:', err);
      }
    });
    res.status(500).json({ message: 'Error al guardar los metadatos del archivo' });
  }
});

const fsPromises = fs.promises;

const emptyDirectory = async (directory) => {
  try {
    const files = await fsPromises.readdir(directory);
    const deletePromises = files.map(async (fileName) => {
      const filePath = path.join(directory, fileName);
      try {
        const stats = await fsPromises.stat(filePath);
        if (stats.isFile()) {
          await fsPromises.unlink(filePath);
          console.log('Archivo eliminado:', filePath);
        } else if (stats.isDirectory()) {
          await emptyDirectory(filePath);
        }
      } catch (err) {  console.error('Error al eliminar archivo o directorio:', err);  }
    });
    await Promise.all(deletePromises);
  } catch (err) {  console.error('Error al leer el directorio:', err);  }
};

const filesOutSubdirectories = async () => {
  const validDirectories = [models3DDir, targetsDir];
  try {
    const files = await fsPromises.readdir(uploadsDir);
    for (const fileName of files) {
      const filePath = path.join(uploadsDir, fileName);
      try {
        const stats = await fsPromises.stat(filePath);
        if (stats.isFile()) {
          const isInValidSubdirectory = validDirectories.some(dir => filePath.startsWith(dir));
          if (!isInValidSubdirectory) {
            await fsPromises.unlink(filePath);
            console.log('Archivo fuera de subdirectorios eliminado:', fileName);
          }
        }
      } catch (err) {  console.error(`Error al procesar archivo ${fileName}: ${err}`);  }
    }
  } catch (err) {  console.error(`Error al leer directorio ${uploadsDir}: ${err}`);  }
};

const checkMisplacedFiles = async () => {
  const validExtensions = {
    [models3DDir]: ['.gltf', '.glb'],
    [targetsDir]: ['.mind']
  };
  const metadata = await loadMetadata();
  for (const [directory, extensions] of Object.entries(validExtensions)) {
    try {
      const files = await fsPromises.readdir(directory);
      for (const fileName of files) {
        const filePath = path.join(directory, fileName);
        try {
          const stats = await fsPromises.stat(filePath);
          if (stats.isFile()) {
            const fileExtension = path.extname(fileName).toLowerCase();
            if (!extensions.includes(fileExtension)) {
              await fsPromises.unlink(filePath);
              console.log(`Archivo en directorio incorrecto eliminado: ${fileName}`);
              if (metadata[fileName]) {
                delete metadata[fileName];
                await saveMetadata(metadata);
              }
            }
          }
        } catch (err) {  console.error(`Error al procesar archivo ${fileName}: ${err}`);  }
      }
    } catch (err) {  console.error(`Error al leer directorio ${directory}: ${err}`);  }
  }
};

const fileMetaMap = async (directory) => {
  if (!fs.existsSync(directory)) {
    console.error(`El directorio ${directory} no existe.`);
    return {};
  }
  const fileMetaMap = {};
  try {
    const files = await fs.promises.readdir(directory);
    for (const fileName of files) {
      const filePath = path.join(directory, fileName);
      try {
        const stats = await fs.promises.stat(filePath);
        if (stats.isFile()) {
          const fileHash = await calculateFileHash(filePath);
          if (!fileMetaMap[fileHash]) {
            fileMetaMap[fileHash] = [];
          }
          fileMetaMap[fileHash].push(fileName);
        }
      } catch (err) {  console.error(`Error al procesar archivo ${fileName}: ${err}`);  }
    }
  } catch (err) {  console.error(`Error al leer directorio ${directory}: ${err}`);  }
  return fileMetaMap;
};

const removeDuplicates = async (fileMetaMap, directory) => {
  try {
    await Promise.all(Object.entries(fileMetaMap).map(async ([hash, fileNames]) => {
      if (fileNames.length > 1) {
        const fileStatsPromises = fileNames.map(async fileName => {
          const filePath = path.join(directory, fileName);
          const stats = await fs.promises.stat(filePath);
          return { fileName, mtimeMs: stats.mtimeMs };
        });
        const fileStats = await Promise.all(fileStatsPromises);
        fileStats.sort((a, b) => b.mtimeMs - a.mtimeMs);
        const duplicates = fileStats.slice(1).map(fileStat => fileStat.fileName);

        await Promise.all(duplicates.map(async (duplicateFileName) => {
          const duplicateFilePath = path.join(directory, duplicateFileName);
          try {
            await fs.promises.unlink(duplicateFilePath);
            console.log(`Archivo duplicado eliminado: ${duplicateFileName}`);
          } catch (err) {
            console.error(`Error al eliminar el archivo duplicado: ${err}`);
          }
          try {
            const metadata = await loadMetadata();
            if (metadata[duplicateFileName]) {
              delete metadata[duplicateFileName];
              await saveMetadata(metadata);
            }
          } catch (err) {  console.error(`Error al manejar los metadatos: ${err}`);  }
        }));
      }
    }));
  } catch (err) {  console.error(`Error durante la eliminación de duplicados: ${err}`);  }
};

const cleanMetadata = async () => {
  try {
    const metadata = await loadMetadata();
    const validDirectories = [models3DDir, targetsDir];
    const filesInUploadsSet = new Set();
    for (const directory of validDirectories) {
      const files = await fs.promises.readdir(directory);
      files.forEach(fileName => filesInUploadsSet.add(fileName));
    }
    const keysToDelete = [];
    Object.keys(metadata).forEach(fileName => {
      if (!filesInUploadsSet.has(fileName)) {
        console.log(`Eliminando metadatos para archivo inexistente: ${fileName}`);
        keysToDelete.push(fileName);
      }
    });
    keysToDelete.forEach(fileName => delete metadata[fileName]);
    await saveMetadata(metadata);
  } catch (err) {  console.error('Error al limpiar los metadatos:', err);  }
};

const maintenance = async () => {
  console.log('Iniciando mantenimiento...');

  const metadata = await loadMetadata();

  if (Object.keys(metadata).length === 0) {
    console.log('Metadatos vacíos. Eliminando archivos remanentes.');
    await emptyDirectory(uploadsDir);
  } else {
    await filesOutSubdirectories();
    await checkMisplacedFiles();

    const models3DFileMetaMap = await fileMetaMap(models3DDir);
    const targetsFileMetaMap = await fileMetaMap(targetsDir);

    await removeDuplicates(models3DFileMetaMap, models3DDir);
    await removeDuplicates(targetsFileMetaMap, targetsDir);

    await cleanMetadata();
  }
  console.log('Mantenimiento completado.');
};

const checkExpiredFiles = async () => {
  try {
    const metadata = await loadMetadata();
    const now = Date.now();
    const oneHour = 60 * 60 * 1000;

    const checkFilesInDirectory = async (directory) => {
      try {
        const files = await fsPromises.readdir(directory);
        const promises = files.map(async (fileName) => {
          const filePath = path.join(directory, fileName);
          try {
            const stats = await fsPromises.stat(filePath);
            if (stats.isFile()) {
              const fileMetadata = metadata[fileName];
              if (fileMetadata && (now - fileMetadata.timestamp > oneHour)) {
                await fsPromises.unlink(filePath);
                console.log('Archivo expirado eliminado:', filePath);
                delete metadata[fileName];
              }
            } else if (stats.isDirectory()) {
              await checkFilesInDirectory(filePath);
            }
          } catch (err) {  console.error('Error al procesar archivo o directorio:', err);  }
        });
        await Promise.all(promises);
      } catch (err) {  console.error('Error al leer el directorio:', err);  }
    };

    await checkFilesInDirectory(targetsDir);
    await checkFilesInDirectory(models3DDir);

    await saveMetadata(metadata); 
  } catch (err) {  console.error('Error al verificar archivos expirados:', err);  }
};

app.use(express.static(path.join(__dirname)));

const startServer = async () => {
  try {
    await maintenance();
    await checkExpiredFiles();
    app.listen(port, () => {
      console.log(`Servidor en funcionamiento en http://localhost:${port}`);
    });
  } catch (err) {
    console.error('Error durante el mantenimiento inicial:', err);
    process.exit(1);
  }
};

startServer();

setInterval(maintenance, 6 * 60 * 60 * 1000);
setInterval(checkExpiredFiles, 60 * 60 * 1000);