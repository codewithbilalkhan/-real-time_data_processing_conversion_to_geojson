from flask import Flask, send_from_directory

app = Flask(__name__)

@app.route('/geojson')
def serve_geojson():
    return send_from_directory('subscriber', 'data.geojson', as_attachment=True)

if __name__ == "__main__":
    app.run(port=5000)
