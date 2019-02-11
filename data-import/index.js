'use strict'
// const AWS = require('aws-sdk')
require('dotenv').config()
const request = require('request')
const SRC_URL = "https://opendata.epa.gov.tw/ws/Data/AQI/\?$format=json"
const Knex = require('knex')

function fetchData() {
    return new Promise((resolve, reject) => {
        request({
            method: 'GET',
            url: SRC_URL,
            json: true,
            agentOptions: {
                rejectUnauthorized: false
            }
        }, (error, response, body) => {
            if(error) {
                return reject(error);
            }
            return resolve(body)
        })
    });
}

function convertNumber(num) {
    return (num === '' || isNaN(num)) ? null : num;
}

async function handlerAsync(event, context) {
    const knex = Knex({
        client: 'pg',
        connection: {
            host: process.env.DB_HOST,
            port: process.env.DB_PORT,
            user: process.env.DB_USER,
            password: process.env.DB_PASS,
            database: process.env.DB_DB
        }
    })
    let rawData = await fetchData();

    console.log('total: ', rawData.length)

    for(let i = 0;i<rawData.length;i++) {
        let raw = rawData[i];
        let data = {
            sitename: raw['SiteName'],
            county: raw['County'],
            aqi:    convertNumber(raw['AQI']) || 0,
            pollutant: raw['Pollutant'],
            status: raw['Status'],
            so2:    convertNumber(raw['SO2']),
            co:     convertNumber(raw['CO']),
            co_8hr: convertNumber(raw['CO_8hr']),
            o3:     convertNumber(raw['O3']),
            o3_8hr: convertNumber(raw['O3_8hr']),
            pm10:   convertNumber(raw['PM10']),
            pm25:   convertNumber(raw['PM2.5']),
            no2:    convertNumber(raw['NO2']),
            nox:    convertNumber(raw['NOx']),
            no:     convertNumber(raw['NO']),
            wind_speed: convertNumber(raw['WindSpeed']),
            wind_direct: convertNumber(raw['WindDirec']),
            publish_time: raw['PublishTime'],
            pm25_avg:   convertNumber(raw['PM2.5_AVG']),
            pm10_avg:   convertNumber(raw['PM10_AVG']),
            so2_avg:    convertNumber(raw['SO2_AVG']),
            longitude: raw['Longitude'],
            latitude: raw['Latitude'],
        }
        console.log("writing", JSON.stringify(data))
        // await knex('twaqi').insert(data)
        await knex.raw('? ON CONFLICT DO NOTHING', [knex('twaqi').insert(data)]) 
    }

    return true;
}

exports.handler = function (event, context, callback) {
    handlerAsync(event, context).then(() => {
        callback();
    }).catch((err) => {
        callback(err);
    })
}
