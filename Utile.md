try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, anno);
			ResultSet rs = st.executeQuery();
			
			while(rs.next()) {
				
			}
			conn.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}