using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClrBasics.Test
{
	public class BatchLookupDemo
	{

		[Test]
		public void MakeMoviePresentationForUser()
		{
			
		}


		public class MovieDB
		{
			public MovieInfo GetMovieInfo(int movieId)
			{
				var movies = new []
				{
					new MovieInfo(1, "Movie 1", 2000),
					new MovieInfo(2, "Movie 2", 2001),
					new MovieInfo(3, "Movie 3", 2002),
					new MovieInfo(4, "Movie 4", 2003),
					new MovieInfo(5, "Movie 5", 2004),
				}.ToDictionary(movie => movie.ID);

				return movies[movieId];
			}
		}

		public class MovieInfo
		{
			public int ID { get; }
			public string Name { get; }
			public int Year { get; }

			public MovieInfo(int id, string name, int year)
			{
				ID = id;
				Name = name;
				Year = year;
			}
		}
	}
}
