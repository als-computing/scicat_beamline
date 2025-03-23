# script for ingesting ALS XMCD data.

# Sample scicat metadata...this is here strictly for requirements gathering
#  at this point
sample_scicat_medata = {
    "motors":
        {
            "motor1": {"start": 0.02, "stop": 0.99, "bin_width": 0.003},
            "fly_motor": "motor1",
            "motor2": [1.0, 2.0, 3.0, 5.0, 6.0] 
        },
    "sample": {
            "postiion": [0.0, 0.0, -3.0],
            "angles": {"theta": 45.0, "azimuth": 0.0},
            "temparature": {"min": 100,  "max": 102, "median": 101},
            "magnetic_field": {"foo": "bar"}
        },
    "beam" : {
        "energy":{
            "beamline": 730,
            "mono" : 790,
            "captured_mono": 810,
            "epu": 33
        },
        "epu_polarization": 42,
        "mono_grating": "??",
        "epu_settings": {
            "gap": 42,
            "A": 42,
            "B": 42
        },
        "m206": {
            "translate": 42,
            "pitch": 42,
            "size": 42
        },
        "incoming_i0_bl": {"min": 100,  "max": 102, "median": 101},
        "incoming_i0_es": {"min": 100,  "max": 102, "median": 101},       
    }
}