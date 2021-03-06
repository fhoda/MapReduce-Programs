air_traffic = LOAD 'air_traffic/input/2008.csv' USING PigStorage(',') 
			AS(Year:chararray,
				Month:chararray,
				DayofMonth:chararray,
				DayofWeek:chararray,
				DepTime:chararray,
				CRSDepTime:chararray,
				ArrTime:chararray,
				CRSArrTime:chararray,
				UniqueCarrier:chararray,
				FlightNum:chararray,
				TailNum:chararray,
				ActualElapsedTime:chararray,
				CRSElapsedTime:chararray,
				AirTime:chararray,
				ArrDelay:int,
				DepDelay:chararray,
				Origin:chararray,
				Dest:chararray,
				Distance:chararray,
				TaxiIn:chararray,
				TaxiOut:chararray,
				Cancelled:chararray,
				CancellationCode:chararray,
				Diverted:chararray,
				CarrierDelay:chararray,
				WeatherDelay:chararray,
				NASDelay:chararray,
				SecurityDelay:chararray,
				LateAircraftDelay:chararray);
