#===============================================================================
#
#  Flatmap viewer and annotation tool
#
#  Copyright (c) 2019  David Brooks
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#===============================================================================

import asyncio
from collections import OrderedDict
from datetime import datetime
import json
from pathlib import Path
import sys

#===============================================================================

import socketio

#===============================================================================

#DATA_DIR = '/Users/dave/build/abi-covid-19/data/Gonzalo/actors_2020_05_05_500_distributed_supermarket'
DATA_DIR = '/Users/dave/build/abi-covid-19/data/Gonzalo/actors_2020_07_05_500_distributed_infection'

## Send simulation bounds as metadata to position/zoom map

#===============================================================================

# Status: S, E, I, R, D for susceptible, exposed, infected, recovered, dead

class ServerConnection(object):
    def __init__(self, host, key):
        self.__host = host
        self.__key = key
        self.__sio = socketio.AsyncClient(engineio_logger=True)
        self.__sio.on('msg', self.recv_message)
        self.__stopped = False

    def recv_message(self, msg):
        if msg['type'] == 'control':
            if msg['data']['type'] == 'simulation':
                if msg['data']['action'] == 'stop':
                    self.__stopped = True

    def __send_message(self, msg_type, data):
        return self.__sio.emit('msg', {'type': msg_type, 'data': data, 'key': self.__key})

    async def closedown(self):
        await self.__send_message('control', {'type': 'simulation', 'action': 'closedown'})
        await self.__sio.wait()

    async def connect(self):
        await self.__sio.connect(self.__host, headers={'key': self.__key})
        self.__stopped = False

    async def send_message(self, msg_type, data):
        await self.__send_message(msg_type, data)
        await self.__sio.sleep(0)               # Will flush send buffer

    def stopped(self):
        return self.__stopped

#===============================================================================

def get_timestamp(file):
    return 1000*datetime(*[int(dt) for dt in file.stem.split('_')[1:]]).timestamp()

#===============================================================================

async def send_simulation(connection, directory):
    simulation = Path(directory)
    sim_name = '_'.join(simulation.stem.split('_')[:4])

    if not (simulation.exists() or simulation.is_dir()):
        await connection.send_data({'error': 'Missing simulation directory'})
        return

    simulation_files = sorted([file for file in simulation.iterdir()
                            if file.suffix == '.geojson'
                           and file.stem.startswith(sim_name)])
    await connection.send_message('metadata', {
        'type': 'simulation',
        'start': get_timestamp(simulation_files[0]),
        'end': get_timestamp(simulation_files[-1]),
        'length': len(simulation_files),
        #'bounds': [(S, W), (N, E)]
    })
    for file in simulation_files:
        if connection.stopped():
            break
        with open(file) as fp:
            feature_collection = json.load(fp)
            if feature_collection['type'] != 'FeatureCollection':
                raise ValueError('Expected a FeatureCollection in {}'.format(file))
            actors = []
            for feature in feature_collection['features']:
                if feature['type'] != 'Feature':
                    raise ValueError('Invalid GeoJSON')
                if feature['properties']['type'] == 'Actor':
                    if feature['geometry']['type'] != 'Point':
                        raise ValueError('Geometry must be a point')
                    actors.append({
                        'id': feature['id'],
                        'position': feature['geometry']['coordinates'],
                        'status': feature['properties']['status'][0].upper()
                        })
            await connection.send_message('data', {
                'type': 'simulation',
                'timestamp': get_timestamp(file),
                'data': actors
            })

#===============================================================================

async def run_simulation(host, key, params):
    conn = ServerConnection(host, key)
    await conn.connect()
    await send_simulation(conn, DATA_DIR)
    await conn.closedown()

#===============================================================================

from simulations.celeryapp import app

@app.task
def run(host, key, params):
    asyncio.run(run_simulation(host, key, params))

#===============================================================================
