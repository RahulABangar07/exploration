@Service
public class UserService {

    @Autowired
    private DataSource dataSource; // Injected HikariDataSource instance

    public Flux<UserDto> getActiveUsers(String department, Integer minAge) {
        String sql = "{call GetActiveUsersProc(?, ?)}"; 

        return ReactiveDBUtility.executeStoredProcedureFlux(
                dataSource, 
                sql, 
                cstmt -> {
                    // This block runs isolated on the boundedElastic thread pool
                    try (ResultSet rs = cstmt.executeQuery()) {
                        List<UserDto> results = new ArrayList<>();
                        while (rs.next()) {
                            results.add(new UserDto(
                                rs.getInt("id"), 
                                rs.getString("name")
                            ));
                        }
                        return results; // Return as iterable list to map to Flux
                    }
                }, 
                department, minAge
        );
    }
}
