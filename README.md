# LHGPM
Cloud Stack Analysis Tool project for CIS 497

Currently, the main bulk of active development files are stored in our GitHub repository. This includes the python script we used to transfer the data from InfluxDB to 
MySQL. This script should not need to be used in the future, but in case re-transferring the data is necessary, instructions on how to run the script and the environment 
needed to do so are in comments on the script. As for the MySQL backups, those files should be on an external drive in the possession of Dr. Benton. If he does not set 
up those files into a MySQL server himself, you will need to restore the backup files. These files have a ‘.sql’ file extension and are essentially queries to restore 
the database. These files are large, and it may take a while to process them. In case this proves unwieldy, we will also include the raw data files pulled from MySQL’s 
data folder.

Our files for our web application are also included in our GitHub repository. This application is currently implemented to run on localhost, and we used XAMPP to 
manage the creation of this localhost server. 

For our MySQL server, there were several user roles that we included. The usernames and passwords for those, as well as their permissions, are as follows:
	User: RyanBenton
		Password: Benton1990
		All permissions
	User: DatabaseManager
		Password: DatabaseManager
		Default database manager permissions
	User: Viewer
		Password: Viewer
		Select and create temporary table permissions

These were set up through the MySQL Workbench, but it is possible to set them up through queries as long as the currently logged in user, typically root, has the 
permission to edit user roles. 

If you do, for some reason, need to reproduce our work in transferring data from InfluxDB to MySQL, you will need a 1.8 installation of InfluxDB. The 1.8 installation 
is near the bottom of that page. You will then need to restore the backups of the InfluxDBs, which are included in our GitHub repository. To restore these backups, you 
will need to run the following commands in your command line:

influxd config
	This will allow you to find the locations of your meta and data directories

influxd restore -database (dbname) -metadir (metadir file path) -datadir (datadir file path) (file path of backup location)
	This will restore the backup files to your InfluxDB database.

If you find a video tutorial easier to follow, here is the one we used: https://www.youtube.com/watch?v=3BhYcOcVRSU&ab_channel=RenuJain

Once the database is in Influx, you can run our python script, following the instructions contained within, to transfer it over to MySQL. 

If you run into any issues with getting our environment replicated, please reach out to Josh Maier (jmaier1999@gmail.com) and I will be more than happy to help. 
