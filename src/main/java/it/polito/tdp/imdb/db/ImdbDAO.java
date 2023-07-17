package it.polito.tdp.imdb.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import it.polito.tdp.imdb.model.Actor;
import it.polito.tdp.imdb.model.Director;
import it.polito.tdp.imdb.model.Movie;

public class ImdbDAO {
	
	public List<Actor> listAllActors(Map<Integer, Actor> idMap){
		String sql = "SELECT * FROM actors";
		List<Actor> result = new ArrayList<Actor>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Actor actor = new Actor(res.getInt("id"), res.getString("first_name"), res.getString("last_name"),
						res.getString("gender"));
				idMap.put(res.getInt("id"), actor);
				result.add(actor);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Movie> listAllMovies(){
		String sql = "SELECT * FROM movies";
		List<Movie> result = new ArrayList<Movie>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Movie movie = new Movie(res.getInt("id"), res.getString("name"), 
						res.getInt("year"), res.getDouble("rank"));
				
				result.add(movie);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	public List<Director> listAllDirectors(Map<Integer, Director> idMap){
		String sql = "SELECT * FROM directors";
		List<Director> result = new ArrayList<Director>();
		
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Director director = new Director(res.getInt("id"), res.getString("first_name"), res.getString("last_name"));
				idMap.put(res.getInt("id"), director);
				result.add(director);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Director> getVertici(Map<Integer, Director> idMap, int anno) {
		
		String sql = "SELECT distinct md.director_id "
				+ "FROM movies_directors md, movies m "
				+ "WHERE m.year = ? AND m.id=md.movie_id";
		List<Director> registi = new LinkedList<>();
		
		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, anno);
			ResultSet rs = st.executeQuery();
			
			while(rs.next()) {
				registi.add(idMap.get(rs.getInt("director_id")));
			}
			conn.close();
			return registi;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
//	public List<Edge> getEdge(int anno, Map<Integer, Director> idMap) {
//		
//		String sql = "(SELECT md1.director_id, md2.director_id, COUNT(*) "
//				+ "FROM movies_directors md1, movies_directors md2, roles r1, roles r2\n"
//				+ "WHERE md1.movie_id!=md2.movie_id AND r1.movie_id=md1.movie_id  \n"
//				+ "		AND r2.movie_id=md2.movie_id AND r1.actor_id=r2.actor_id AND md1.director_id < md2.director_id \n"
//				+ "		AND md1.movie_id IN (SELECT id FROM movies WHERE YEAR = 2005) \n"
//				+ "		AND md2.movie_id IN (SELECT id FROM movies WHERE YEAR = 2005)\n"
//				+ "GROUP BY md1.director_id, md2.director_id\n"
//				+ ")\n"
//				+ "UNION \n"
//				+ "(SELECT md1.director_id, md2.director_id, COUNT(*)\n"
//				+ "FROM movies_directors md1, movies_directors md2, roles r\n"
//				+ "WHERE md1.movie_id=md2.movie_id AND md1.director_id < md2.director_id AND md1.movie_id = r.movie_id \n"
//				+ "		AND md2.movie_id=r.movie_id \n"
//				+ "		AND md1.movie_id IN (SELECT id FROM movies WHERE YEAR = 2005) \n"
//				+ "		AND md2.movie_id IN (SELECT id FROM movies WHERE YEAR = 2005)\n"
//				+ "GROUP BY md1.director_id, md2.director_id)";
//	}
	
	public Map<Director, Set<Actor>> getEdge(int anno, Map<Integer, Director> idMapRegisti, Map<Integer, Actor> idMapAttori) {
		
		String sql = "SELECT md.director_id, r.actor_id "
				+ "FROM movies_directors md, movies m, roles r "
				+ "WHERE m.year = ? AND md.movie_id=r.movie_id AND m.id=md.movie_id";
		
		Map<Director, Set<Actor>> mappa = new TreeMap<Director, Set<Actor>>();
		
		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, anno);
			ResultSet rs = st.executeQuery();
			
			while(rs.next()) {
				
				Director regista = idMapRegisti.get(rs.getInt("director_id"));
				Actor attore = idMapAttori.get(rs.getInt("actor_id"));
				
				if(mappa.containsKey(regista)){ 
					mappa.get(regista).add(attore);
				} else {
					Set<Actor> attori = new HashSet<Actor>();
					attori.add(attore);
					mappa.put(regista, attori);
				}
			}
			
			conn.close();
			return mappa;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	
	
}
