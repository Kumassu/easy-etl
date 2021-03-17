package song.pan.etl.rdbms.element;

import lombok.Getter;
import lombok.Setter;
import song.pan.etl.common.exception.InvalidSQLException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public class SQLResolver {

    public static Select resolveSelect(String stmt) {
        String prune = prune(stmt).toUpperCase();
        Select select = new Select();
        select.setSelectItem(extract(stmt, prune, ItemType.SELECT));
        select.setFromItem(extract(stmt, prune, ItemType.FROM));
        select.setWhereItem(extract(stmt, prune, ItemType.WHERE));
        select.setGroupByItem(extract(stmt, prune, ItemType.GROUP_BY));
        select.setHavingItem(extract(stmt, prune, ItemType.HAVING));
        select.setOrderByItem(extract(stmt, prune, ItemType.ORDER_BY));
        return select;
    }


    static String prune(String stmt) {
        while (stmt.contains("(")) {
            int leftBracket = 0;
            int rightBracket = 0;
            for (int i = 0; i < stmt.length(); i++) {
                if (stmt.charAt(i) == '(') {
                    leftBracket = i;
                }
                if (stmt.charAt(i) == ')') {
                    rightBracket = i;
                    break;
                }
            }
            StringBuilder sb = new StringBuilder(stmt);
            sb.replace(leftBracket, rightBracket + 1, IntStream.range(leftBracket, rightBracket + 1).mapToObj(i -> "_").collect(Collectors.joining()));
            stmt = sb.toString();
            System.out.println(stmt);
        }
        return stmt;
    }


    static Item extract(String stmt, String trunk, ItemType itemType) {
        if (!trunk.contains(itemType.keyWord)) {
            return null;
        }

        int begin = trunk.indexOf(itemType.keyWord);

        ItemType nextItem = null;
        for (ItemType type : ItemType.values()) {
            if (type.ordinal() > itemType.ordinal() && trunk.contains(type.keyWord)) {
                nextItem = type;
                break;
            }
        }
        int end;
        if (null == nextItem) {
            end = trunk.length();
        } else {
            end = trunk.indexOf(nextItem.keyWord);
        }

        String content = stmt.substring(begin + itemType.keyWord.length(), end);
        String contentTrunk = trunk.substring(begin + itemType.keyWord.length(), end);
        String[] membersTrunk = contentTrunk.split(itemType.delimiter);
        List<String> members = new LinkedList<>();

        for (int i = 0; i < membersTrunk.length; i++) {
            int start = IntStream.range(0, i).reduce(0, (left, right) -> left + membersTrunk[right].length() + itemType.delimiter.length());
            members.add(content.substring(start, start + membersTrunk[i].length()));
        }
        Item item = new Item(itemType);
        item.setMembers(members);
        return item;
    }


    public enum ItemType {
        SELECT("SELECT ", ","),
        FROM(" FROM ", ","),
        WHERE(" WHERE ", " AND "),
        GROUP_BY(" GROUP BY ", ","),
        HAVING(" HAVING ", " AND "),
        ORDER_BY(" ORDER BY ", ","),
        ;
        private String keyWord;
        private String delimiter;

        ItemType(String keyWord, String delimiter) {
            this.keyWord = keyWord;
            this.delimiter = delimiter;
        }
    }

    @Getter
    @Setter
    public static class Item {
        private ItemType type;
        private List<String> members;

        public Item(ItemType type) {
            this.type = type;
            this.members = new LinkedList<>();
        }

        @Override
        public String toString() {
            return type.keyWord + String.join(type.delimiter, members);
        }
    }


    @Getter
    @Setter
    public static class Select {
        private Item selectItem;
        private Item fromItem;
        private Item whereItem;
        private Item groupByItem;
        private Item havingItem;
        private Item orderByItem;

        @Override
        public String toString() {
            return Stream.of(selectItem, fromItem, whereItem, groupByItem, havingItem, orderByItem)
                    .filter(Objects::nonNull).map(Item::toString).collect(Collectors.joining());
        }
    }

    public static void main(String[] args) {
        resolveSelect("SELECT ID,Name,CountryCode,District,Population FROM world.city");
    }

}
