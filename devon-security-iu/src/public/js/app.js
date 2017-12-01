
var interactiveMap = new Vue({
  el: '#interactive-map',
  data: {
    callReponse: {},
    coordinates: [],
    geoJson: {}
  },
  methods: {
    getCoords: function(){
      getCoords(1, this.coordinates, this.geoJson);
    }
  }
});

var mymap = L.map('mapid').setView([32.25559, -103.1933], 16);

function getCoords(id, coordinates, geoJson){
  console.log(id);

  var elasticQuery = {
    "_source": ["location", "timestamp"],
    "query": {
      "bool": {
        "must": [{
          "geo_bounding_box": {
            "location": {
              "top_left": {
                "lat": 33.0,
                "lon": -104.0
              },
              "bottom_right": {
                "lat": 32.0,
                "lon": -103.0
              }
            }
          }
        },
          {
            "range": {
              "timestamp": {
                "gt": "now-1m"
              }
            }
          }
        ]
      }
    },
    "sort" : [{ "timestamp" : {"order" : "asc"}}]
  }

  axios.get('http://elastic:9200/transportation/_search?size=100&filter_path=hits.hits.*', elasticQuery).then(response => {
    callReponse = response.data['hits']['hits'];
    coordinates = this.coordinates = [];
    console.log(callReponse);

    geoJson["type"]="LineString";
    console.log(geoJson);

    for (var i in this.callReponse) {
      var record = this.callReponse[i]['_source'];
      coordinates.push([record['location']['lat'], record['location']['lon']]);
    }

    geoJson["coordinates"]=coordinates;

    L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
      attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
      maxZoom: 18,
      id: 'mapbox.streets',
      accessToken: 'pk.eyJ1IjoiYWxleHdvb2xmb3JkIiwiYSI6ImNpd3gxOXFudjAxYTAyb3NhbjBzNjM2d2gifQ.J03qzuTFa7fktiNMQLyAQg'
    }).addTo(mymap);

    var polyline = L.polyline(coordinates, {color: 'red'}).addTo(mymap);

    console.log(geoJson);

  });

}

window.onload = function() {
  var visits = 0;

  var visitsCookie = getCookieValue('visits');
  if (visitsCookie) {
    var converted = parseInt(visitsCookie);
    if (!isNaN(converted)) {
      visits = converted;
    }
  }

  setCookieValue('visits', (visits+1));
};

function getCookieValue(id) {
  var name = id + '=';
  var decodedCookie = decodeURIComponent(document.cookie);

  var kvps = decodedCookie.split(';');
  for(var i = 0; i <kvps.length; i++) {
    var v = kvps[i];
    while (v.charAt(0) == ' ') {
      v = v.substring(1);
    }

    if (v.indexOf(id) == 0) {
      var result = v.substring((id.length+1), v.length);
      return result;
    }
  }
  return '';
}

function setCookieValue(id, v) {
  var d = new Date();
  d.setTime(d.getTime() + (365*24*60*60*1000));

  var expires = 'expires=' +  d.toUTCString();
  var cookieValue = id + "=" + v + ";" + expires + ";path=/";
  console.log('cookieValue: ' + cookieValue);
  document.cookie = cookieValue;
}
