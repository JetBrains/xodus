/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

import java.io.*;

public class PersistentStoreExample {

    public static final String BLOG_ENTITY_TYPE_NAME = "Blog";
    public static final String POST_ENTITY_TYPE_NAME = "Post";

    private static final String SAMPLE_BLOG_POST =
            "This is sample blog post about Xodus!\n" +
                    "\n" +
                    "Xodus is already here!";

    public static void main(String[] args) {
        final ClassLoader classLoader = PersistentStoreExample.class.getClassLoader();

        //Create or open persistent store under directory "data"
        final PersistentEntityStoreImpl store = PersistentEntityStores.newInstance("data");

        // Create new blog and put new blog post there
        final EntityId blogId = store.computeInTransaction(txn -> {
            final Entity blog = txn.newEntity(BLOG_ENTITY_TYPE_NAME);
            blog.setProperty("name", "Xodus Official Blog");
            final Entity post;

            try {
                post = createNewBlogPost(txn, "Xodus blog post",
                    new ByteArrayInputStream(SAMPLE_BLOG_POST.getBytes("UTF-8")));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            blog.addLink("items", post);
            return blog.getId();
        });

        // Load blog and show posts and print content
        store.executeInTransaction(txn -> {
            final EntityIterable blogs = txn.getAll(BLOG_ENTITY_TYPE_NAME);
            for (Entity blog : blogs) {
                System.out.println("Blog name: " + blog.getProperty("name"));
                final Iterable<Entity> blogItems = blog.getLinks("items");

                for (Entity item : blogItems) {
                    try {
                        printBlogItem(item);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

        });

        // Close store when we are done
        store.close();
    }

    private static void printBlogItem(Entity item) throws IOException {
        System.out.println("\tPost title: " + item.getProperty("title"));
        final InputStream content = item.getBlob("content");
        System.out.println("\tPost content: ");
        String line;
        try (BufferedReader contentReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(content)))) {
            while ((line = contentReader.readLine()) != null) {
                System.out.println("\t" + line);
            }
        }
    }


    private static Entity createNewBlogPost(StoreTransaction txn, String title, InputStream content) {
        final Entity post = txn.newEntity(POST_ENTITY_TYPE_NAME);
        post.setProperty("title", title);
        post.setBlob("content", content);
        return post;
    }
}
