import kagglehub

global_environmental_trends = kagglehub.dataset_download("adilshamim8/temperature")
dog_images = kagglehub.dataset_download("yaswanthgali/dog-images")
yelp = kagglehub.dataset_download("yelp-dataset/yelp-dataset")
electric_vehicle_pop = kagglehub.dataset_download("adarshde/electric-vehicle-population-dataset")

global_environmental_trends_path = (global_environmental_trends + "\\temperature.csv")
#dog_images_path = (dog_images + "\r")
#yelp_path = (yelp + "\yelp_academic_dataset_checkin.json")
#electric_vehicle_pop_path = ()

print("Path to global_environmental_trends dataset files:", global_environmental_trends_path)
# print("Path to dog_images dataset files:", dog_images_path)
# print("Path to yelp dataset files:", yelp_path)
# print("Path to electric_vehicle_population dataset files:", electric_vehicle_pop_path)

def get_data():
    return global_environmental_trends_path#, dog_images_path, yelp_path, electric_vehicle_pop_path

#C:\\Users\\tomgr\\.cache\\kagglehub\\datasets\\yelp-dataset\\yelp-dataset\\versions\\4' 