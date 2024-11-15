from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import math

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://weather_data_user:bronzed1234@34.172.251.28:5432/postgres'

# Configure two databases
app.config['SQLALCHEMY_BINDS'] = {
    'data_db': 'postgresql://weather_data_user:bronzed1234@35.226.56.142:5432/postgres',  # Data DB for Data info
    'app_db': 'postgresql://weather_data_user:bronzed1234@34.172.251.28:5432/postgres'   # App DB for users and ride info
}

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = '62b952a59c392d4441a9970e62a0064d081ee376245f2a759bdc3991486310ae'  # Secure JWT key
app.config['SECRET_KEY'] = '40496e33-4556-4b82-927b-a3674fa85794'

timestamp_format = "%Y-%m-%d %H:%M:%S"

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
jwt = JWTManager(app)

def get_last_15_minute_timestamp(timestamp=None):
    # Get the current time
    if timestamp == None:
        timestamp = datetime.now()
    minutes = timestamp.minute
    minutes_rounded = (minutes // 15) * 15
    last_15_minute_timestamp = timestamp.replace(minute=minutes_rounded, second=0, microsecond=0)
    return last_15_minute_timestamp


def get_temperature(time_of_day, weather_condition):
    """
    Function to estimate the temperature based on time of day, month, and weather condition in India.

    Args:
    - time_of_day (datetime): Time of day for which we want to estimate the temperature.
    - month (int): Month of the year (1 to 12).
    - weather_condition (str): Weather condition ('clear', 'cloudy', 'rainy', etc.)

    Returns:
    - float: Estimated temperature in Celsius.
    """
    month = time_of_day.month
    # Base temperature range depending on the month (in °C)
    # These are rough estimates of temperatures during different seasons
    if 3 <= month <= 5:  # Summer (March - May)
        min_temp = 25
        max_temp = 40
    elif 6 <= month <= 9:  # Monsoon (June - September)
        min_temp = 22
        max_temp = 35
    elif 10 <= month <= 11:  # Post-Monsoon/Fall (October - November)
        min_temp = 15
        max_temp = 30
    else:  # Winter (December - February)
        min_temp = 10
        max_temp = 25
    
    # Calculate the hour of the day (from 0 to 23)
    hour = time_of_day.hour
    
    # Normalize the time to a 24-hour format where:
    #  - 6 AM = 0, Noon = 6, 6 PM = 12, Midnight = 18
    normalized_time = (hour - 6) % 24
    
    # Use a sine function to model temperature variation throughout the day
    # Sine function will range between -1 and 1, so scale it to the desired temperature range.
    temperature_factor = math.sin(math.radians(normalized_time * 360 / 24))  # normalized time over 24 hours
    
    # Scale the sine wave output to range from min_temp to max_temp
    temp = min_temp + (max_temp - min_temp) * (temperature_factor + 1) / 2
    
    # Adjust temperature based on weather conditions
    if weather_condition == 'clear':
        temp += 2  # Clear skies can increase temperature by 2°C
    elif weather_condition == 'cloudy':
        temp -= 2  # Cloudy conditions can lower temperature by 2°C
    elif weather_condition == 'rainy':
        temp -= 3  # Rainy conditions can lower temperature by 3°C
    
    # Ensure the temperature is within reasonable bounds
    temp = max(temp, min_temp)  # Don't let it go below min_temp
    temp = min(temp, max_temp)  # Don't let it go above max_temp
    
    return round(temp, 5)

class CloudData(db.Model):
    __bind_key__ = 'data_db'
    __tablename__ = 'ct'  # Specify the existing table name
    __table_args__ = (db.PrimaryKeyConstraint('timestamp', 'ct'),
                      {'schema': 'data_forecast' })  # Specify the schema name

    lat = db.Column(db.Float,nullable=True)
    lon = db.Column(db.Float,nullable=True)
    ct = db.Column(db.Integer,nullable=True)
    timestamp = db.Column(db.DateTime, nullable=False)
    st = db.Column(db.String(10),nullable=False)

# User Model (stored in the primary 'users_db')
class User(db.Model):
    __bind_key__ = 'app_db'
    __table_args__ = {'schema': 'api'} 
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)
    role = db.Column(db.String(20), nullable=False)  # 'admin' or 'transporter'

    def to_dict(self):
        return {'id': self.id, 'username': self.username, 'role': self.role}
    

# Ride Model (stored in the secondary 'rides_db')
class Ride(db.Model):
    __bind_key__ = 'app_db'
    __table_args__ = {'schema': 'api'} 
    id = db.Column(db.Integer, primary_key=True)
    transporter_id = db.Column(db.Integer, nullable=False)  # Foreign key to User (not enforced in sqlite)
    origin = db.Column(db.String(100), nullable=False)
    destination = db.Column(db.String(100), nullable=False)
    start_lat = db.Column(db.Float, nullable=False)
    start_lon = db.Column(db.Float, nullable=False)
    end_lat = db.Column(db.Float, nullable=False)
    end_lon = db.Column(db.Float, nullable=False)
    distance = db.Column(db.Float, nullable=True)
    timestamp = db.Column(db.DateTime, default=db.func.current_timestamp())
    status = db.Column(db.String(20), default='Pending')
    """
    'Pending'
    'In Progress'
    'Completed'
    'Cancelled'
    """
    def to_dict(self):
        return {
            'id': self.id,
            'transporter_id': self.transporter_id,
            'origin': self.origin,
            'destination': self.destination,
            'start_lat': self.start_lat,
            'start_lon': self.start_lon,
            'end_lat': self.end_lat,
            'end_lon': self.end_lon,
            'distance': self.distance,
            'timestamp': self.timestamp
        }

# Updated endpoint to add a ride (only for transporters)
@app.route('/add_ride', methods=['POST'])
@jwt_required()
def add_ride():
    data = request.get_json()
    current_user = get_jwt_identity()

    # Only allow transporters to add rides
    user = User.query.filter_by(username=current_user['username']).first()
    if user.role != 'transporter':
        return jsonify({"error": "Unauthorized"}), 403

    # Capture start and end coordinates from request data
    new_ride = Ride(
        transporter_id=user.id,
        origin=data['origin'],
        destination=data['destination'],
        start_lat=data['start_lat'],
        start_lon=data['start_lon'],
        end_lat=data['end_lat'],
        end_lon=data['end_lon'],
        distance=data['distance']
    )
    db.session.add(new_ride)
    db.session.commit()
    return jsonify(new_ride.to_dict()), 201




# Endpoint to register a new user
@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data['username']
    password = data['password']
    role = data['role']

    # Check if an admin already exists
    if role == 'admin' and User.query.filter_by(role='admin').first():
        return jsonify({"error": "Admin user already exists"}), 400

    # Hash password and create a user
    hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')
    new_user = User(username=username, password=hashed_password, role=role)
    db.session.add(new_user)
    db.session.commit()
    return jsonify(new_user.to_dict()), 201


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data['username']
    password = data['password']

    user = User.query.filter_by(username=username).first()
    if user and bcrypt.check_password_hash(user.password, password):
        access_token = create_access_token(identity={'username': user.username, 'role': user.role},expires_delta=timedelta(days=24))
        return jsonify(access_token=access_token), 200
    else:
        return jsonify({"error": "Invalid username or password"}), 401
    
    
# Endpoint to get all rides for a transporter
@app.route('/my_rides', methods=['GET'])
@jwt_required()
def my_rides():
    current_user = get_jwt_identity()
    user = User.query.filter_by(username=current_user['username']).first()
    
    if user.role != 'transporter':
        return jsonify({"error": "Unauthorized"}), 403

    rides = Ride.query.filter_by(transporter_id=user.id).all()
    return jsonify([ride.to_dict() for ride in rides]), 200

@app.route('/get_location_data',methods=['GET'])
def get_location_data():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    timestamp = request.args.get('timestamp', type=str)

    timestamp = datetime.strptime(timestamp,timestamp_format)
    lat_max = lat + 0.0449
    lat_min = lat - 0.0449
    lon_max = lon + 0.0449
    lon_min = lon - 0.0449
    timestamp = get_last_15_minute_timestamp(timestamp=timestamp)
    print(lat_max,lat_min,lon_min,timestamp)
    cloud_data = CloudData.query.\
        filter(CloudData.timestamp == timestamp).\
            filter(CloudData.lat < lat_max ).\
                filter(CloudData.lat >= lat_min).\
                    filter(CloudData.lon>= lon_min).all()
    cloud_data = [{"lat": data.lat, "lon": data.lon, "timestamp": data.timestamp,'ct':data.ct} for data in cloud_data]
    cloud_data = pd.DataFrame(cloud_data)
    # print(cloud_data)
    if len(cloud_data)>0:
        cloud_data['lat_diff'] = cloud_data['lat'] - lat
        cloud_data['lat_diff'] = cloud_data['lat_diff'].abs()
        cloud_data['lon_diff'] = cloud_data['lon'] - lon
        cloud_data['lon_diff'] = cloud_data['lon_diff'].abs()
        cloud_data['offset'] = cloud_data['lat_diff'] + cloud_data['lon_diff']
        cloud_data.sort_values('offset',ascending=True,inplace=True)
        cloud_data = cloud_data.head(1)
        # ct = cloud_data.head(1)['ct']
        cloud_data['lat'] = lat
        cloud_data['lon'] = lon
        cloud_data['timestamp'] = timestamp
        # cloud_data['']
    else:
        cloud_data = pd.DataFrame({'ct':None,'lat':[lat],'lon':[lon],"timestamp":timestamp})
    # get ct data
    ct = list(cloud_data['ct'])[0]
    climate = 'clear'
    if ct != None and ct < 4:
        climate = 'clear'
    else:
        climate = 'cloudy'
    
    temp = get_temperature(time_of_day=timestamp,weather_condition=climate)
    cloud_data['temp'] = temp
    cloud_data['cloud_top_temp'] = None
    send_cols = ['lat','lon','timestamp','temp','cloud_top_temp','ct']

    cloud_data = cloud_data.loc[:,send_cols]
    cloud_data.fillna('NULL',inplace=True)
    # print(cloud_data)
    return jsonify(cloud_data.to_dict('records'),200)
    


if __name__ == '__main__':
    
    with app.app_context() as ctx:
        #ctx.push()# Ensure the app context is available
        db.create_all()
        print("Done This!")
        app.run(debug=True,port=5400)
    