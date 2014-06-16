package jetbrains.exodus.env;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ExodusException;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.env.StoreConfig.WITH_DUPLICATES;

public class Users {

    public static void main(String[] args) {

        final Environment env = Environments.newInstance("data");
        try {
            final Store users = env.computeInTransaction(new TransactionalComputable<Store>() {
                @Override
                public Store compute(@NotNull final Transaction txn) {
                    return env.openStore("Users", WITH_DUPLICATES, txn);
                }
            });
            final Store emails = env.computeInTransaction(new TransactionalComputable<Store>() {
                @Override
                public Store compute(@NotNull final Transaction txn) {
                    return env.openStore("Emails", WITH_DUPLICATES, txn);
                }
            });

            if (args.length == 0) {
                listAllUsers(env, users);
                return;
            }

            if (args.length == 1 && !isHelpQuery(args[0])) {
                if (args[0].contains("@")) {
                    final String email = args[0];
                    listUsersBy(env, emails, email);
                } else {
                    final String username = args[0];
                    listUsersBy(env, users, username);
                }
                return;
            }

            if (args.length == 2) {
                registerNewUser(env, users, emails, args[0], args[1]);
                return;
            }

            System.out.println("Usage:");
            System.out.println("  Users <no params> - list all users;");
            System.out.println("  Users <username | email> - list users with specified username or email;");
            System.out.println("  Users <username> <e-mail> - register new user.");

        } finally {
            env.close();
        }
    }

    private static void listAllUsers(final Environment env, final Store users) {
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final Cursor cursor = users.openCursor(txn);
                try {
                    long count = 0;
                    while (cursor.getNext()) {
                        System.out.println(entryToString(cursor.getKey()) + ' ' + entryToString(cursor.getValue()));
                        ++count;
                    }
                    System.out.println("Total users: " + count);
                } finally {
                    cursor.close();
                }
            }
        });
    }

    private static void listUsersBy(final Environment env, final Store store, final String key) {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final ArrayByteIterable emailEntry = stringToEntry(key);
                final Cursor cursor = store.openCursor(txn);
                try {
                    if (cursor.getSearchKey(emailEntry) != null) {
                        boolean hasNext = true;
                        int i = 0;
                        for (; hasNext; ++i, hasNext = cursor.getNext()) {
                            if (!key.equalsIgnoreCase(entryToString(cursor.getKey()))) {
                                break;
                            }
                            System.out.println(entryToString(cursor.getValue()) + ' ' + key);
                        }
                        System.out.println("Total found: " + i);
                    } else if (cursor.getSearchKeyRange(emailEntry) != null) {
                        System.out.println(key + " not found, nearest candidates: ");
                        boolean hasNext = true;
                        for (; hasNext; hasNext = cursor.getNext()) {
                            final String currentKey = entryToString(cursor.getKey());
                            if (!currentKey.startsWith(key)) {
                                break;
                            }
                            System.out.println(entryToString(cursor.getValue()) + ' ' + currentKey);
                        }
                    } else {
                        System.out.println("Nothing found");
                    }
                } finally {
                    cursor.close();
                }
            }
        });
    }

    private static void registerNewUser(final Environment env, final Store users, final Store emails, final String username, final String email) {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final ArrayByteIterable usernameEntry = stringToEntry(username);
                final ArrayByteIterable emailEntry = stringToEntry(email);
                final boolean exists;
                final Cursor usersCursor = users.openCursor(txn);
                try {
                    exists = usersCursor.getSearchBoth(usernameEntry, emailEntry);
                    if (!exists) {
                        users.put(txn, usernameEntry, emailEntry);
                    }
                } finally {
                    usersCursor.close();
                }
                if (!exists) {
                    final Cursor emailsCursor = emails.openCursor(txn);
                    try {
                        if (emailsCursor.getSearchBoth(emailEntry, usernameEntry)) {
                            throw new ExodusException("It can't be: users & emails are inconsistent!");
                        }
                        emails.put(txn, emailEntry, usernameEntry);
                    } finally {
                        emailsCursor.close();
                    }
                }
                System.out.println((exists ? "User is already registered: " : "New user registered: ") + username + " " + email);
            }
        });
    }

    private static boolean isHelpQuery(@NotNull final String query) {
        return query.equalsIgnoreCase("/help") || query.equalsIgnoreCase("/h") ||
                query.equalsIgnoreCase("-help") || query.equalsIgnoreCase("-h") ||
                query.equalsIgnoreCase("--help") || query.equalsIgnoreCase("--h");
    }
}
