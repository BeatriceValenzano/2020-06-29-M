package it.polito.tdp.imdb.model;

import java.util.*;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import it.polito.tdp.imdb.db.ImdbDAO;

public class Model {

	SimpleWeightedGraph <Director, DefaultWeightedEdge> grafo;
	Map<Integer, Director> idMapRegisti;
	Map<Integer, Actor> idMapAttori;
	Map<Director, Set<Actor>> mappaRegistiAttori;
	ImdbDAO dao;
	
	public Model() {
		grafo = new SimpleWeightedGraph <Director, DefaultWeightedEdge> (DefaultWeightedEdge.class);
		dao = new ImdbDAO();
		this.idMapRegisti = new TreeMap<Integer, Director>();
		this.idMapAttori = new TreeMap<Integer, Actor>();
		this.dao.listAllDirectors(idMapRegisti);	
		this.dao.listAllActors(idMapAttori);

	}
	
	public void creaGrafo(int anno) {
		
		grafo = new SimpleWeightedGraph <Director, DefaultWeightedEdge> (DefaultWeightedEdge.class);
		
		
//		AGGIUNTA VERTICI
		Graphs.addAllVertices(grafo, dao.getVertici(idMapRegisti, anno));
		
//		AGGIUNTA ARCHI
		this.mappaRegistiAttori = new HashMap<Director, Set<Actor>>(dao.getEdge(anno, idMapRegisti, idMapAttori));
		for(Director d1 : grafo.vertexSet()) {
			for(Director d2 : grafo.vertexSet()) {
				if(!d1.equals(d2)) {
					//Prendo i due set
					Set<Actor> set1 = new HashSet<Actor>(this.mappaRegistiAttori.get(d1));
					Set<Actor> set2 = new HashSet<Actor>(this.mappaRegistiAttori.get(d2));
					set1.retainAll(set2); //tiene gli elementi presenti sia in uno che nell'altro set
					if(set1.size() > 0) {
						Graphs.addEdge(grafo, d1, d2, set1.size());
					}
				}
			}
		}
	}
	
	public int getVertici() {
		return grafo.vertexSet().size();
	}
	
	public int getEdge() {
		return grafo.edgeSet().size();
	}
}
