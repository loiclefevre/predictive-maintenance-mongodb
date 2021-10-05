const schedule = require('node-schedule');
const mongoose = require('mongoose');
const { ObjectId } = require('mongoose');
mongoose.set('debug',true);

if(!process.env.PASSWORD) {
    console.error('Environment variable PASSWORD is not set!');
    exit;
}

// Connection URI
// Standard MongoDB database
const mongoDB_URI = 'mongodb://predmain:' + process.env.PASSWORD + '@132.145.157.51:27017/predmain?authenticationDatabase=admin&compressors=zlib&gssapiServiceName=mongodb';
// Oracle Database API for MongoDB with AJD-S
//const mongoDB_URI = 'mongodb://predmain:' + process.env.PASSWORD + '@nnrtbqrbdeylh1o-loic.adb-preprod.us-phoenix-1.oraclecloudapps.com:27016/predmain?authMechanism=PLAIN&readPreference=primary&authSource=%24external&appname=MongoDB+Compass&directConnection=true&loadBalanced=false&ssl=true';

if(!mongoDB_URI) {
    console.error('MongoDB URI not set!');
    exit;
}

// Mongoose Models
const deviceMetricsSchema = new mongoose.Schema({
    id_machine: String,
    sampleperiod: Date,
    cast_bar_temperature: Number,
    solube_oil_temperature: Number,
    casting_water_temprature: Number,
    flue_gaz_temperature: Number,
    iron_fe: Number,
    mill_rpm: Number,
    cathode_voltage: Number,
    casting_rate: Number,
    silicon_si: Number,
    ambient_temperature: Number,
    lube_oil_temperature: Number,
    cast_wheel_speed: Number
  });

function nextInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min) + min); //The maximum is exclusive and the minimum is inclusive
}

function nextDouble(min, max) {
    return Math.random() * (max - min) + min;
}

function nextBoolean() {
    return Math.random() < 0.5 ? false : true;
}

deviceMetricsSchema.methods.initialize = function initialize() {
    this.solube_oil_temperature = nextDouble(40, 62);
    this.casting_water_temprature = nextDouble(1, 99);
    this.mill_rpm = nextInt(1500, 1600);
    this.casting_rate = nextDouble(7.59, 8.13);
    this.cast_wheel_speed = nextDouble(1244, 1365);
    this.lube_oil_temperature = nextInt(40, 50);
    this.ambient_temperature = nextDouble(140, 193);


    this.silicon_si = nextDouble(80, 192);

    this.cast_bar_temperature = nextDouble(180, 209);
    this.flue_gaz_temperature = nextDouble(0, 180.18);
    this.iron_fe = nextDouble(0, 193.31);
    this.cathode_voltage = nextDouble(90,130);
}

deviceMetricsSchema.methods.work = function work() {
    var workers = deviceMetricsWorkersMap.get( this.id_machine );

    var randomLife = nextInt(0,100);

    if(randomLife <= 2) {
        if(workers.size < 4) {
            switch(nextInt(0,5)) {
                case 0:
                    if(!workers.has('CastBarTemperatureUpdater'))
                    workers.set('CastBarTemperatureUpdater', { step: (nextBoolean() ? -1.0 : 1.0) * nextDouble(2,3), numberOfSteps: nextInt(5,10), currentStep: 0});
                    break;

                case 1:
                    if(!workers.has('SolubeOilTemperatureUpdater'))
                    workers.set('SolubeOilTemperatureUpdater', { step: (nextBoolean() ? -1.0 : 1.0) * nextDouble(2,3), numberOfSteps: nextInt(10,15), currentStep: 0});
                    break;

                case 2:
                    if(!workers.has('CastingWaterTemperatureUpdater'))
                    workers.set('CastingWaterTemperatureUpdater', { step: (nextBoolean() ? -1.0 : 1.0) * nextDouble(2,3), numberOfSteps: nextInt(3,5), currentStep: 0});
                    break;

                case 3:
                    if(!workers.has('FlueGasTemperatureUpdater'))
                    workers.set('FlueGasTemperatureUpdater', { step: (nextBoolean() ? -1.0 : 1.0) * nextDouble(2,3), numberOfSteps: nextInt(10,15), currentStep: 0});
                    break;

                case 4:
                    if(!workers.has('CathodeVoltageUpdater'))
                    workers.set('CathodeVoltageUpdater', { step: nextDouble(3,5), numberOfSteps: nextInt(10,15), currentStep: 0});
                    break;
            }
        }
    }

    // console.log(randomLife+' Device '+this.id_machine+' has '+workers.size+' workers...')

    workers.forEach( (w, key, map) => {
        if(w.currentStep < w.numberOfSteps) {

            switch(key) {
                case 'CastBarTemperatureUpdater':
                    this.cast_bar_temperature += w.step;
                    break;

                case 'SolubeOilTemperatureUpdater':
                    this.solube_oil_temperature += w.step;
                    break;

                case 'CastingWaterTemperatureUpdater':
                    this.casting_water_temprature += w.step;
                    break;

                case 'FlueGasTemperatureUpdater':
                    this.flue_gaz_temperature += w.step;
                    break;

                case 'CathodeVoltageUpdater':
                    if(this.cathode_voltage < 260) {
                        this.cathode_voltage += w.step;
                    }
                    break;
            }

            w.currentStep++;
        } else {
            map.delete(key);
        }
    } );
}

function simulate(id_machine) {
    var dm = deviceMetricsMap.get( id_machine );
    dm.work();

    return new DeviceMetrics( {
        _id: new mongoose.Types.ObjectId(),
        id_machine: dm.id_machine,
        sampleperiod: new Date(),
        cast_bar_temperature: dm.cast_bar_temperature,
        solube_oil_temperature: dm.solube_oil_temperature,
        casting_water_temprature: dm.casting_water_temprature,
        flue_gaz_temperature: dm.flue_gaz_temperature,
        iron_fe: dm.iron_fe,
        mill_rpm: dm.mill_rpm,
        cathode_voltage: dm.cathode_voltage,
        casting_rate: dm.casting_rate,
        silicon_si: dm.silicon_si,
        ambient_temperature: dm.ambient_temperature,
        lube_oil_temperature: dm.lube_oil_temperature,
        cast_wheel_speed: dm.cast_wheel_speed
        } );
}

const DeviceMetrics = mongoose.model('Device_Metrics', deviceMetricsSchema);

var deviceMetricsMap;
var deviceMetrics;

const deviceSchema = new mongoose.Schema({
    id_machine: String
  });

const Device = mongoose.model('Device', deviceSchema);

const Statistics = mongoose.model('Statistics', new mongoose.Schema({
    time: { type: Date },
    origin: {
        type: String,
        required: 'Origin is required'
      },
    total: { type: Number, required: 'Total number of documents is required' },
    docsPerSecond: { type: Number, required: 'Number of documents loaded per second is required' },
    megaBytesPerSecond: { type: Number, required: 'Mega Bytes of documents loaded per second is required' }
  }));

var sizeCounter = 0;

async function main() {
    console.info('Starting predictive maintenance loader...');

    await mongoose.connect(mongoDB_URI, { useNewUrlParser: true, useCreateIndex: true, useUnifiedTopology: true, useFindAndModify: false });

    console.info('Initializing devices...');

    // Retrieve all devices to manage
    const devices = await Device.find().select('id_machine');

    console.info(`Devices initialization: done (${devices.length} device(s) registered)`);

    deviceMetricsMap = new Map();
    deviceMetrics = new Array();
    deviceMetricsWorkersMap = new Map();
    for (var i = 0; i < devices.length; i++) { 
        const dm = new DeviceMetrics({ id_machine: devices[i].id_machine });
        dm.initialize();
        deviceMetrics.push( dm );
        deviceMetricsMap.set( dm.id_machine, dm );
        deviceMetricsWorkersMap.set( dm.id_machine, new Map() );
    }

    //console.log(JSON.stringify(devices));
    let initialDocumentscount = await Statistics.findOne().sort('-time').limit(1).select('total');

    if(!initialDocumentscount) {
        initialDocumentscount = 0;
    } else {
        initialDocumentscount = initialDocumentscount.total;
    }

    console.info(`Current device metrics count initialization: done (${initialDocumentscount} document(s) found)`);

    // Start job to generate random device metrics every 10 seconds
    const job = schedule.scheduleJob('*/10 * * * * *', function() {
        var startTime = new Date();
        initialDocumentscount = loadRandomDeviceMetrics(devices, initialDocumentscount);
        var endTime = new Date();

        // Saving process duration
        new Statistics({
            time: new Date(),
            origin: 'device_metrics',
            total: initialDocumentscount,
            docsPerSecond: devices.length / ((endTime-startTime)/1000.0),
            megaBytesPerSecond: (sizeCounter/(1024.0*1024.0)) / ((endTime-startTime)/1000.0)
        }).save();
      });
}

function loadRandomDeviceMetrics(devices, initialDocumentscount) {
    var bulk = DeviceMetrics.collection.initializeOrderedBulkOp();
    sizeCounter = 0;

    deviceMetrics.forEach((d,i) => {
        const deviceMetric = simulate( d.id_machine );
        sizeCounter += JSON.stringify(deviceMetric).length;
        bulk.insert( deviceMetric );
    } );

    bulk.execute(function(err,result) {
        // do something with the result here
     });

    console.log('Loaded devices metrics...'+ (initialDocumentscount+devices.length));

    return initialDocumentscount + devices.length;
}

main().catch(err => console.log(err));
