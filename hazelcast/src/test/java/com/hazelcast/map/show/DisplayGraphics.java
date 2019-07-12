package com.hazelcast.map.show;

import javax.swing.*;
import java.awt.*;
import java.util.LinkedList;
import java.util.Queue;

public class DisplayGraphics extends Canvas {

    private final Queue<Integer> evictedKeys;

    public DisplayGraphics(Queue<Integer> evictedKeys) {
        this.evictedKeys = evictedKeys;
    }

    @Override
    public void paint(Graphics g) {
        Plate plate1 = new Plate(ItemCount.P_500, 1, g);
        Plate plate2 = new Plate(ItemCount.P_500, 2, g);

        int falsePositives = 0;
        int correct = 0;
        for (Integer key : evictedKeys) {
            if (key >= 0 && key < 1000) {
                plate2.put(Type.NOT_EVICT);
                falsePositives++;
            } else if (key >= 1000 && key < 1500) {
                plate1.put(Type.EVICTED);
                correct++;
            }
        }

        System.err.println("falsePositives=" + falsePositives + ", correct=" + correct);
    }

    private class Plate {
        private final int w;
        private final int h;
        private final int graphWidth;
        private final int graphHeight;
        private final int fixedDivisionHeight;

        private final int yFirst;
        private final Graphics g;

        private final LinkedList<int[]> positions = new LinkedList<>();

        public Plate(ItemCount itemCount, int number, Graphics g) {
            this.g = g;
            this.graphHeight = itemCount.graphHeight();
            this.graphWidth = itemCount.graphWidth();
            this.w = itemCount.width();
            this.h = itemCount.height();
            assert graphHeight % 2 == 0;
            this.fixedDivisionHeight = graphHeight / 2;
            this.yFirst = (number - 1) * fixedDivisionHeight;

            for (int y = yFirst + h; y <= yFirst + fixedDivisionHeight; y += h) {
                for (int x = 0; x < graphWidth; x += w) {
                    positions.add(new int[]{x, y});
                }
            }
        }

        public void put(Type type) {
            int[] xy = positions.poll();
            color(type, g, w, h, xy[0], xy[1]);
        }
    }

    private static void color(Type type, Graphics g, int w, int h, int x, int y) {
        g.setColor(type.getColor());
        g.fillOval(x, y, w, h);
    }

    public void showGraph() {
        JFrame f = new JFrame();
        f.add(this);
        f.setSize(800, 700);
        //f.setLayout(null);
        f.setVisible(true);
    }

    public enum Type {
        EVICTED {
            @Override
            Color getColor() {
                return Color.BLACK;
            }
        },
        NEXT {
            @Override
            Color getColor() {
                return Color.GRAY;
            }
        },
        NOT_EVICT {
            @Override
            Color getColor() {
                return Color.RED;
            }
        };

        abstract Color getColor();
    }

    private enum ItemCount {
        P_500 {
            int width() {
                return 10;
            }

            int height() {
                return 10;
            }

            int graphWidth() {
                return 500;
            }

            int graphHeight() {
                return 300;
            }
        },

        P_10_000 {
            int width() {
                return 10;
            }

            int height() {
                return 10;
            }

            int graphWidth() {
                return 500;
            }

            int graphHeight() {
                return 300;
            }
        };

        abstract int width();

        abstract int height();

        abstract int graphWidth();

        abstract int graphHeight();
    }
}